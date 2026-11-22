"""Unit tests for the AWS benchmark/failover logic (no AWS, no Docker)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "infra/aws-1region/node"), str(ROOT / "infra/aws-1region/scripts")]

import cdc_remote  # noqa: E402
import connector_watchdog as wd  # noqa: E402
import run_aws_benchmark as drv  # noqa: E402
from bench_lib import bursts, followup, longest_gap, soak_summary, soak_verdict  # noqa: E402
from bench_lib import entry_matches, join_lags, mismatches, pctl, summarize, verdict  # noqa: E402

ROW = {"name": "x", "price": "1.00", "stock": 3}
OPS = {"update": 400, "delete": 50, "reinsert": 40, "noop": 10}


# ---------------------------------------------------------------- bench_lib
def test_pctl():
    assert pctl([1, 2, 3, 4], 50) == 2.5
    assert pctl([5], 99) == 5
    assert pctl([], 50) != pctl([], 50)     # NaN


def test_join_lags_ignores_unmatched_versions():
    commits = [[1, 1, 100.0], [1, 2, 101.0], [2, 1, 102.0]]
    seen = [[1, 2, 101.5], [2, 1, 102.25], [9, 9, 1.0]]   # (1,1) coalesced; (9,9) not ours
    assert join_lags(commits, seen) == [500.0, 250.0]


@pytest.mark.parametrize("want,entry,ok", [
    (ROW, {"deleted": False, "payload": {**ROW, "id": 1}}, True),
    (ROW, {"deleted": False, "payload": {**ROW, "stock": 2}}, False),
    (ROW, {"deleted": True, "payload": None}, False),
    (ROW, None, False),
    (ROW, {"deleted": False, "payload": None}, False),
    (None, None, True),
    (None, {"deleted": True, "payload": None}, True),
    (None, {"deleted": False, "payload": ROW}, False),
])
def test_entry_matches(want, entry, ok):
    assert entry_matches(want, entry) is ok


def test_mismatches_counts_missing_keys():
    truth = {"1": ROW, "2": ROW, "3": None}
    assert mismatches(truth, {"1": {"deleted": False, "payload": ROW}}) == ["2"]


def obs(bad=(), seen=((1, 1, 100.1),)):
    return {"seen": [list(s) for s in seen], "total": 50, "mismatched": list(bad), "converge_s": 1.0}


def test_verdict_pass_and_each_failure():
    commits = [[1, 1, 100.0]]
    good = summarize("a", commits, obs())
    assert verdict([good], {"op_counts": OPS}) == (True, [])
    assert not verdict([], {"op_counts": OPS})[0]
    assert not verdict([summarize("a", commits, obs(bad=["7"]))], {"op_counts": OPS})[0]
    assert not verdict([summarize("a", commits, obs(seen=()))], {"op_counts": OPS})[0]
    skewed = summarize("a", commits, obs(seen=((1, 1, 99.0),)))
    assert "negative lag" in verdict([skewed], {"op_counts": OPS})[1][0]
    assert not verdict([good], {"op_counts": {**OPS, "delete": 0}})[0]


# ---------------------------------------------------------------- driver
class FakeTransport:
    def __init__(self, files: dict, rc=None):
        self.files, self.rc, self.calls = files, rc or {}, []

    def start(self, side, args, timeout_s=900):
        self.calls.append((side, args[0]))
        return side, args[0]

    def wait(self, handle, timeout_s=900):
        return self.rc.get(handle, 0), "", ""

    def fetch(self, name):
        return json.dumps(self.files[name.split("/")[1]])


def files(**over):
    return {"writes.json": {"commits": [[1, 1, 100.0]], "op_counts": OPS, "duration_s": 2.0},
            "observe-a.json": obs(), "observe-b.json": obs(), **over}


def test_driver_pass_starts_observer_before_writer():
    t = FakeTransport(files())
    r, ok = drv.benchmark(t, "r")
    assert ok and [s["side"] for s in r["sides"]] == ["a", "b"]
    assert t.calls == [("a", "bench-prepare"), ("b", "bench-observe"), ("a", "bench-run")]


def test_driver_fails_if_one_side_is_missing():
    f = files()
    del f["observe-b.json"]
    r, ok = drv.benchmark(FakeTransport(f), "r")
    assert not ok and any("incomplete" in p for p in r["problems"])


def test_driver_fails_on_nonzero_node_exit_even_if_data_looks_fine():
    assert not drv.benchmark(FakeTransport(files(), {("b", "bench-observe"): 1}), "r")[1]


def test_driver_aborts_if_prepare_fails():
    with pytest.raises(SystemExit):
        drv.benchmark(FakeTransport(files(), {("a", "bench-prepare"): 1}), "r")


# ---------------------------------------------------------------- connector watchdog
OK_DB = {"read_write": True, "slot": True, "wal_status": "reserved"}


@pytest.mark.parametrize("states,db,action", [
    (["RUNNING", "RUNNING"], None, "ok"),
    (["RUNNING", "UNASSIGNED"], OK_DB, "ok"),
    (["RUNNING", "FAILED"], OK_DB, "restart"),
    (["FAILED"], {**OK_DB, "wal_status": "extended"}, "restart"),
    (["RUNNING", "FAILED"], None, "wait"),                                   # database still down
    (["RUNNING", "FAILED"], {**OK_DB, "read_write": False}, "wait"),         # still on the old writer
    (["RUNNING", "FAILED"], {**OK_DB, "slot": False, "wal_status": None}, "alert"),
    (["RUNNING", "FAILED"], {**OK_DB, "wal_status": "lost"}, "alert"),
    (["RUNNING", "FAILED"], {**OK_DB, "wal_status": "unreserved"}, "alert"),
])
def test_watchdog_restarts_only_when_no_event_can_be_skipped(states, db, action):
    assert wd.decide(states, db)[0] == action


def test_watchdog_never_touches_a_healthy_connector_even_if_the_slot_is_gone():
    assert wd.decide(["RUNNING", "RUNNING"], {**OK_DB, "slot": False})[0] == "ok"


# ---------------------------------------------------------------- SSM transport
NODES = {"region_a": "us-east-2", "region_b": "us-east-2", "instance_a": "i-a", "instance_b": "i-b", "bucket": "bk"}


def test_ssm_quotes_args_and_targets_the_right_node(monkeypatch):
    calls = []
    monkeypatch.setattr(cdc_remote, "aws", lambda region, *a: calls.append(a) or "cid\n")
    assert cdc_remote.Ssm(NODES).start("b", ["bench-observe", "x; rm -rf /"]) == ("b", "cid")
    args = calls[0]
    assert args[args.index("--instance-ids") + 1] == "i-b"
    script = json.loads(args[args.index("--parameters") + 1])["commands"][0]
    assert script == "/opt/cdc/cdcctl.sh bench-observe 'x; rm -rf /'"


@pytest.mark.parametrize("status,code,want", [
    ("Success", 0, 0), ("Failed", 3, 3), ("TimedOut", -1, 255), ("Cancelled", 0, 255)])
def test_ssm_wait_maps_status_and_survives_late_registration(monkeypatch, status, code, want):
    seq = [cdc_remote.RemoteError("InvocationDoesNotExist"),
           {"Status": "InProgress"},
           {"Status": status, "ResponseCode": code, "StandardOutputContent": "o", "StandardErrorContent": "e"}]

    def fake(region, *a):
        x = seq.pop(0)
        if isinstance(x, Exception):
            raise x
        return json.dumps(x)

    monkeypatch.setattr(cdc_remote, "aws", fake)
    assert cdc_remote.Ssm(NODES, poll_s=0).wait(("a", "cid")) == (want, "o", "e")


def test_ssm_wait_propagates_other_errors(monkeypatch):
    def boom(region, *a):
        raise cdc_remote.RemoteError("AccessDenied")
    monkeypatch.setattr(cdc_remote, "aws", boom)
    with pytest.raises(cdc_remote.RemoteError, match="AccessDenied"):
        cdc_remote.Ssm(NODES, poll_s=0).wait(("a", "cid"))


# ---------------------------------------------------------------- failover soak
def test_followup_mix_and_longest_gap():
    kinds = [followup(n) for n in range(20)]
    assert (kinds.count("update"), kinds.count("delete"), kinds.count(None)) == (5, 3, 12)
    assert longest_gap([1.0, 10.0, 2.0]) == (8.0, 2.0)
    assert longest_gap([1.0]) == (0.0, 0.0)


def test_bursts_counts_only_real_pauses():
    assert bursts([0.0, 0.01, 1.01, 1.02, 2.02]) == {"pauses": 2, "median_pause_ms": 1000.0}
    assert bursts([1.0]) == {"pauses": 0, "median_pause_ms": 0.0}


def soak_inputs():
    w = {"keys": 3, "inserts": [[1, 0, 10.0], [2, 1, 11.0], [3, 2, 30.0]], "acks": [10.0, 11.0, 30.0],
         "errors": [[12.0, "OperationalError"], [20.0, "OperationalError"]], "failed": [], "db_missing": [],
         "t0": 9.0, "t_end": 31.0}
    v = {"seen": [[1, 0, 10.5], [2, 1, 11.5], [3, 2, 31.0]], "total": 3, "mismatched": [], "missing": [],
         "converge_s": 1.0}
    st = {"cache_epoch": "1", "topic": "t", "slot": "s", "connector": "RUNNING/RUNNING"}
    return w, {"a": dict(v), "b": dict(v)}, {"a": dict(st), "b": dict(st)}


FO = {"old_writer": "x", "new_writer": "y"}


def test_soak_summary_and_pass():
    w, sides, st = soak_inputs()
    s = soak_summary(w, sides)
    assert s["longest_write_pause_s"] == 19.0 and s["error_window_s"] == 8.0
    assert s["a"]["lag_max_s"] == 1.0 and s["b"]["longest_cache_pause_s"] == 19.5
    assert soak_verdict(s, st, st, FO) == (True, [])


@pytest.mark.parametrize("mutate,expect", [
    (lambda w, v, a: v["b"].update(mismatched=["2"], missing=["2"]), "b: 1 keys differ"),
    (lambda w, v, a: v["a"].update(total=2), "verified 2 of 3"),
    (lambda w, v, a: w.update(db_missing=[1]), "missing from Aurora"),
    (lambda w, v, a: w.update(failed=["INSERT"]), "gave up"),
    (lambda w, v, a: w["inserts"].pop(), "only 2 of 3"),
    (lambda w, v, a: w.update(errors=[]), "did not overlap"),
    (lambda w, v, a: v.pop("b"), "no result from side b"),
    (lambda w, v, a: a["a"].update(cache_epoch="2"), "cache_epoch changed"),
    (lambda w, v, a: a["a"].update(slot="s2"), "slot changed"),
    (lambda w, v, a: a["a"].update(connector="RUNNING/FAILED"), "connector is"),
])
def test_soak_verdict_detects(mutate, expect):
    w, sides, before = soak_inputs()
    after = {k: dict(x) for k, x in before.items()}
    mutate(w, sides, after)
    ok, problems = soak_verdict(soak_summary(w, sides), before, after, FO)
    assert not ok and any(expect in p for p in problems), problems


def test_crash_may_recover_in_place_but_must_interrupt_writes():
    w, sides, st = soak_inputs()
    same = {"mode": "crash", "old_writer": "x", "new_writer": "x"}
    assert soak_verdict(soak_summary(w, sides), st, st, same) == (True, [])
    assert not soak_verdict(soak_summary({**w, "errors": []}, sides), st, st, same)[0]
    assert "not injected" in soak_verdict(soak_summary(w, sides), st, st, {**same, "crash_failed": "denied"})[1][0]


def test_soak_verdict_requires_a_real_failover():
    w, sides, st = soak_inputs()
    ok, problems = soak_verdict(soak_summary(w, sides), st, st, {"old_writer": "x", "new_writer": "x"})
    assert not ok and "writer did not change" in problems[0]
