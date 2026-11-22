"""Unit tests for the version-gated Lua script, read path, event parsing and slot monitor.

Core invariant: an older (epoch, LSN) never overwrites a newer one.
Runs against the local Redis (db 15) so real cache keys are untouched.
"""
import itertools
import json
import os
import random

import pytest
import redis

from worker.consumer import LAG_WINDOW, MalformedEvent, Metrics, WalLagMonitor, parse_event
from worker.invalidator import EPOCH_KEY, EpochRegression, Invalidator

TEST_DB = int(os.getenv("REDIS_TEST_DB", "15"))


@pytest.fixture
def r():
    client = redis.Redis(host=os.getenv("REDIS_HOST", "127.0.0.1"), port=6379, db=TEST_DB, decode_responses=True,
                         password=os.getenv("REDIS_PASSWORD", "cdc-local-dev") or None)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not reachable")
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def make(r):
    def _make(epoch=0, **kw):
        return Invalidator(client=r, key_prefix="t:", ttl_seconds=60, fill_ttl_seconds=30, epoch=epoch, **kw)
    return _make


@pytest.fixture
def inv(make):
    return make()


# ------------------------------------------------------------- LSN ordering
def test_first_write_applies(inv):
    assert inv.upsert(1, {"v": 1}, 100)
    e = inv.get(1)
    assert e.lsn == 100 and e.payload == {"v": 1} and not e.deleted and not e.fill


def test_newer_overwrites_older(inv):
    assert inv.upsert(1, {"v": 1}, 100)
    assert inv.upsert(1, {"v": 2}, 200)
    assert inv.get(1).payload == {"v": 2}


def test_older_never_overwrites_newer(inv):
    assert inv.upsert(1, {"v": 2}, 200)
    assert not inv.upsert(1, {"v": 1}, 100)
    e = inv.get(1)
    assert e.lsn == 200 and e.payload == {"v": 2}


def test_duplicate_lsn_is_rejected(inv):
    assert inv.upsert(1, {"v": 1}, 100)
    assert not inv.upsert(1, {"v": "dup"}, 100)
    assert inv.get(1).payload == {"v": 1}


def test_delete_tombstone_blocks_late_update(inv):
    assert inv.upsert(1, {"v": 1}, 100)
    assert inv.delete(1, 300)
    assert not inv.upsert(1, {"v": 2}, 200), "delayed update must not resurrect a deleted row"
    e = inv.get(1)
    assert e.deleted and e.lsn == 300 and e.payload is None


def test_stale_delete_is_rejected(inv):
    assert inv.upsert(1, {"v": 2}, 200)
    assert not inv.delete(1, 100)
    assert not inv.get(1).deleted


def test_reinsert_after_delete(inv):
    assert inv.delete(1, 100)
    assert inv.upsert(1, {"v": "back"}, 200)
    assert inv.get(1).payload == {"v": "back"}


def test_ttl_is_set(inv):
    inv.upsert(1, {"v": 1}, 100)
    assert 0 < inv.r.ttl("t:1") <= 60


def test_keys_are_independent(inv):
    assert inv.upsert(1, {"v": 1}, 500)
    assert inv.upsert(2, {"v": 1}, 100)  # lower LSN on a different key is fine


@pytest.mark.parametrize("lsn_hi,lsn_lo", [
    (2**53 + 1, 2**53),               # beyond double precision
    (2**64 - 1, 2**64 - 2),           # max u64
    (10_000_000_000, 9_999_999_999),  # different digit length
])
def test_large_lsn_comparison_is_exact(inv, lsn_hi, lsn_lo):
    assert inv.upsert(1, {"v": "hi"}, lsn_hi)
    assert not inv.upsert(1, {"v": "lo"}, lsn_lo)
    assert inv.get(1).payload == {"v": "hi"}


def test_all_permutations_converge_to_max_lsn(inv):
    events = [(100, "set"), (200, "set"), (300, "del"), (400, "set")]
    for perm in itertools.permutations(events):
        inv.r.flushdb()
        for lsn, op in perm:
            inv.delete(1, lsn) if op == "del" else inv.upsert(1, {"lsn": lsn}, lsn)
        e = inv.get(1)
        assert e.lsn == 400 and e.payload == {"lsn": 400}, perm


def test_random_shuffle_with_duplicates_converges(inv):
    rng = random.Random(42)
    for key in range(50):
        lsns = rng.sample(range(1, 10_000), 20)
        stream = lsns + rng.choices(lsns, k=10)  # duplicates
        rng.shuffle(stream)
        for lsn in stream:
            inv.upsert(key, {"lsn": lsn}, lsn)
        assert inv.get(key).lsn == max(lsns)


def test_invalid_args_rejected_by_script(inv):
    k = ["t:x", inv.watermark_key]
    with pytest.raises(redis.ResponseError):
        inv._script(keys=k, args=["set", "{}", "0", "-5", 0])
    with pytest.raises(redis.ResponseError):
        inv._script(keys=k, args=["set", "{}", "x", "5", 0])
    with pytest.raises(redis.ResponseError):
        inv._script(keys=k, args=["bogus", "{}", "0", "5", 0])


# ------------------------------------------------------------------- epochs
def test_new_epoch_wins_even_with_lower_lsn(make):
    old, new = make(epoch=1), make(epoch=2)
    assert old.upsert(1, {"v": "old-db"}, 9_000_000)
    assert new.upsert(1, {"v": "restored-db"}, 100), "restored DB has lower LSNs but a newer epoch"
    assert new.get(1).payload == {"v": "restored-db"}


def test_old_epoch_event_never_overwrites_new_epoch(make):
    old, new = make(epoch=1), make(epoch=2)
    assert new.upsert(1, {"v": "new"}, 100)
    assert not old.upsert(1, {"v": "late-old"}, 9_000_000)
    assert not old.delete(1, 9_000_000)
    assert new.get(1).payload == {"v": "new"}


def test_entries_without_epoch_field_are_epoch_zero(inv, make):
    inv.r.hset("t:1", mapping={"lsn": "500", "payload": "{}", "deleted": "0"})  # pre-epoch format
    assert not inv.upsert(1, {"v": 1}, 400)
    assert make(epoch=1).upsert(1, {"v": 2}, 1)


def test_ensure_epoch_records_and_refuses_regression(make):
    make(epoch=3).ensure_epoch()
    assert make(epoch=3).r.get(EPOCH_KEY) == "3"
    make(epoch=4).ensure_epoch()
    with pytest.raises(EpochRegression):
        make(epoch=3).ensure_epoch()


# ----------------------------------------------------------- read path (fill)
def test_fill_populates_missing_key(inv):
    assert inv.fill(1, {"v": "db"})
    e = inv.get(1)
    assert e.fill and e.lsn == 0 and e.payload == {"v": "db"}
    assert 0 < inv.r.ttl("t:1") <= 30


def test_fill_never_overwrites_existing_entry(inv):
    assert inv.upsert(1, {"v": "cdc"}, 100)
    assert not inv.fill(1, {"v": "reader"})
    assert inv.get(1).payload == {"v": "cdc"}


def test_fill_does_not_overwrite_tombstone(inv):
    assert inv.delete(1, 100)
    assert not inv.fill(1, {"v": "reader-saw-pre-delete-row"})
    assert inv.get(1).deleted


def test_any_cdc_event_supersedes_fill(inv):
    """The race that pg_current_wal_lsn() fills got wrong: a reader reads v1 while
    an uncommitted v2 is already in WAL. The CDC event for v2 must still win."""
    assert inv.fill(1, {"v": 1})
    assert inv.upsert(1, {"v": 2}, 1)  # smallest possible real LSN
    e = inv.get(1)
    assert e.payload == {"v": 2} and not e.fill


def test_cdc_delete_supersedes_fill(inv):
    assert inv.fill(1, {"v": 1})
    assert inv.delete(1, 50)
    assert inv.get(1).deleted


def test_old_epoch_event_loses_to_new_epoch_fill(make):
    new, old = make(epoch=2), make(epoch=1)
    assert new.fill(1, {"v": "read-from-restored-db"})
    assert not old.upsert(1, {"v": "late-old-db-event"}, 9_000_000)


def test_read_through_miss_hit_and_tombstone(inv):
    calls = []
    loader = lambda pk: calls.append(pk) or {"id": pk, "v": "db"}  # noqa: E731
    assert inv.read_through(7, loader) == {"id": 7, "v": "db"}
    assert inv.read_through(7, loader) == {"id": 7, "v": "db"}
    assert calls == [7], "second read must be a cache hit"
    inv.delete(7, 10)
    assert inv.read_through(7, loader) is None and calls == [7], "tombstone is a negative hit"


def test_read_through_absent_row_is_not_cached(inv):
    assert inv.read_through(8, lambda pk: None) is None
    assert inv.r.exists("t:8") == 0


# ----------------------------------------------------------------- truncate
def test_truncate_hides_existing_entries(inv):
    inv.upsert(1, {"v": 1}, 100)
    inv.delete(2, 110)
    assert inv.truncate(200)
    assert inv.get(1) is None and inv.get(2) is None


def test_truncate_rejects_delayed_pre_truncate_events(inv):
    assert inv.truncate(200)
    assert not inv.upsert(1, {"v": "pre-truncate"}, 150)
    assert not inv.delete(1, 150)
    assert inv.get(1) is None


def test_post_truncate_events_apply(inv):
    inv.upsert(1, {"v": "old"}, 100)
    assert inv.truncate(200)
    assert inv.upsert(1, {"v": "new"}, 250)
    assert inv.get(1).payload == {"v": "new"}


def test_truncate_watermark_only_moves_forward(inv):
    assert inv.truncate(300)
    assert not inv.truncate(200)
    assert not inv.truncate(300)
    assert inv.truncate(400)


def test_fill_after_truncate_is_valid_and_superseded_by_cdc(inv):
    inv.upsert(1, {"v": "pre"}, 100)
    inv.truncate(200)
    assert inv.fill(1, {"v": "reader-post-truncate"}), "pre-truncate leftover counts as absent"
    e = inv.get(1)
    assert e.fill and e.payload == {"v": "reader-post-truncate"}
    assert inv.upsert(1, {"v": "cdc"}, 201)
    assert inv.get(1).payload == {"v": "cdc"}


def test_pre_truncate_fill_is_invalidated(inv):
    assert inv.fill(1, {"v": "reader-pre-truncate"})
    assert inv.truncate(200)
    assert inv.get(1) is None


def test_new_epoch_clears_truncate_barrier(make):
    old, new = make(epoch=1), make(epoch=2)
    assert old.truncate(9_000_000)
    assert new.upsert(1, {"v": "restored"}, 5)
    assert new.get(1).payload == {"v": "restored"}


# ------------------------------------------------------------ event parsing
def _env(op, before=None, after=None, lsn=123, **src):
    return json.dumps({"op": op, "before": before, "after": after,
                       "source": {"lsn": lsn, "ts_ms": 1, **src}}).encode()


@pytest.mark.parametrize("op", ["c", "u", "r"])
def test_parse_upserts(op):
    ev = parse_event(_env(op, after={"id": 7, "name": "x"}), b'{"id":7}')
    assert ev.op == op and ev.pk == 7 and ev.lsn == 123 and ev.row["name"] == "x"


def test_parse_delete_uses_before_image():
    ev = parse_event(_env("d", before={"id": 9}), b'{"id":9}')
    assert ev.op == "d" and ev.pk == 9 and ev.row is None


def test_parse_truncate_has_no_image_or_key():
    ev = parse_event(_env("t"), None)
    assert ev.op == "t" and ev.pk is None and ev.lsn == 123


def test_parse_kafka_tombstone_is_none():
    assert parse_event(None, b'{"id":1}') is None


@pytest.mark.parametrize("value", [
    b"not json",
    b"[1,2]",
    _env("x", after={"id": 1}),
    _env("u", after={"id": 1}, lsn=None),
    _env("u", after={"id": 1}, lsn="123"),
    _env("t", lsn=None),
    _env("u", after=None),
    _env("u", after={"name": "no pk"}),
])
def test_parse_malformed_raises(value):
    with pytest.raises(MalformedEvent):
        parse_event(value, None)


# ----------------------------------------------------------------- metrics
def test_lag_metrics_are_bounded():
    m = Metrics()
    for i in range(LAG_WINDOW * 3):
        m.lag_ms.append(float(i))
    assert len(m.lag_ms) == LAG_WINDOW
    snap = m.snapshot()
    assert snap["lag_p50_ms"] is not None and snap["lag_p99_ms"] >= snap["lag_p50_ms"]


# ------------------------------------------------------------ slot monitor
def _mon(**kw):
    alerts = []
    m = WalLagMonitor("unused", threshold_bytes=1000, interval_s=1, slot_name="s",
                      inactive_grace_s=60, on_alert=lambda slot, msg: alerts.append((slot, msg)), **kw)
    return m, alerts


def test_monitor_healthy_slot_no_alert():
    m, alerts = _mon()
    m.evaluate([("s", True, "reserved", 10)], now=0)
    assert alerts == []


def test_monitor_lag_over_threshold():
    m, alerts = _mon()
    m.evaluate([("s", True, "extended", 5000)], now=0)
    assert any("retained WAL" in a[1] for a in alerts)


@pytest.mark.parametrize("status", ["lost", "unreserved"])
def test_monitor_lost_slot_alerts_even_with_null_lag(status):
    m, alerts = _mon()
    m.evaluate([("s", False, status, None)], now=0)
    assert any(f"wal_status={status}" in a[1] for a in alerts)


def test_monitor_inactive_grace_period():
    m, alerts = _mon()
    m.evaluate([("s", False, "reserved", 10)], now=0)
    m.evaluate([("s", False, "reserved", 10)], now=30)
    assert alerts == []
    m.evaluate([("s", False, "reserved", 10)], now=61)
    assert any("inactive" in a[1] for a in alerts)
    alerts.clear()
    m.evaluate([("s", True, "reserved", 10)], now=62)
    m.evaluate([("s", False, "reserved", 10)], now=63)
    assert alerts == [], "grace timer resets once the slot is active again"


def test_monitor_missing_slot():
    m, alerts = _mon()
    m.evaluate([], now=0)
    assert alerts == [("s", "slot missing: CDC is not running")]


# ------------------------------------------------- batching and coalescing
from worker.consumer import ChangeEvent, coalesce  # noqa: E402


def _ev(op, pk, lsn):
    return ChangeEvent(op=op, pk=pk, lsn=lsn, row=None if op in "dt" else {"lsn": lsn}, source_ts_ms=0)


def test_coalesce_keeps_max_lsn_per_key_and_truncate():
    evs = [_ev("u", 1, 10), _ev("u", 1, 30), _ev("d", 1, 20), _ev("u", 2, 5),
           _ev("t", None, 7), _ev("t", None, 9)]
    out, superseded = coalesce(evs)
    got = {(e.op, e.pk): e.lsn for e in out}
    assert got == {("u", 1): 30, ("u", 2): 5, ("t", None): 9} and superseded == 3


def test_apply_batch_matches_sequential_semantics(inv):
    res = inv.apply_batch([("c", 1, {"v": 1}, 100), ("u", 1, {"v": 0}, 50),   # stale
                           ("u", 1, {"v": 1}, 100),                          # duplicate
                           ("d", 2, None, 200), ("u", 2, {"v": "late"}, 150),  # tombstone holds
                           ("c", 3, {"v": 3}, 10), ("t", None, None, 20),     # truncate hides 3
                           ("u", 3, {"v": "pre"}, 15)])                       # pre-truncate: rejected
    assert res == [True, False, False, True, False, True, True, False]
    assert inv.get(1).payload == {"v": 1}
    assert inv.get(2).deleted
    assert inv.get(3) is None


@pytest.mark.parametrize("seed", range(5))
def test_coalesced_batch_equals_unbatched_final_state(make, r, seed):
    """Coalescing + pipelining must reach exactly the same visible state as
    applying every event one by one in arrival order."""
    rng = random.Random(seed)
    evs = []
    for lsn in rng.sample(range(1, 5000), 300):
        op = rng.choice("uuuuudt") if rng.random() > 0.02 else "t"
        evs.append(_ev(op, None if op == "t" else rng.randint(1, 20), lsn))
    a = make()
    for e in evs:
        a.apply_batch([(e.op, e.pk, e.row, e.lsn)])
    seq = {pk: a.get(pk) for pk in range(1, 21)}
    r.flushdb()
    b = make()
    out, _ = coalesce(evs)
    b.apply_batch([(e.op, e.pk, e.row, e.lsn) for e in out], chunk=7)
    assert {pk: b.get(pk) for pk in range(1, 21)} == seq
