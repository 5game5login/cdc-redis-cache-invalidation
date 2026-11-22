"""Continuous writes across an Aurora failover, WITHOUT an epoch bump: is anything lost?

    python infra/aws-1region/scripts/run_failover_soak.py --duration 240 --out results/aws/x.json
    python ... run_failover_soak.py --no-failover --duration 30      # validate the test itself
    python ... run_failover_soak.py --crash --rate 200               # crash the writer, heavier load

PASS needs: every acked insert still in Aurora, every key equal to Aurora in both
caches, nothing gave up, the same epoch/slot/topic before and after, the connector
running, and (with failover) a writer that really changed while writes were in flight.
With --crash the writer may recover in place, so only the interruption is required.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / "node")]

from bench_lib import soak_summary, soak_verdict  # noqa: E402
from cdc_remote import Ssm, aws, run  # noqa: E402


def status(t: Ssm, side: str) -> dict:
    rc, out, err = run(t, side, ["status"])
    if rc != 0:
        raise SystemExit(f"status {side} failed: {err or out}")
    return json.loads(out.strip().splitlines()[-1])


def writer(n: dict) -> str:
    q = "DBClusters[0].DBClusterMembers[?IsClusterWriter].DBInstanceIdentifier | [0]"
    return aws(n["region_a"], "rds", "describe-db-clusters", "--db-cluster-identifier", n["cluster_id"],
               "--query", q, "--output", "text").strip()


KEEP = re.compile(r"Producer failure|Going to restart|Attempting to restart task|SnapshotResult|"
                  r"WAL resume position|Processing messages")


def timeline(n: dict, t_call: float, errors: list) -> dict:
    """Debezium's view of the interruption, in seconds after the call. No addresses kept."""
    out = aws(n["region_a"], "logs", "filter-log-events", "--log-group-name", "/cdc-poc-a/node",
              "--log-stream-names", "cdc-a-connect-1", "--start-time", str(int((t_call - 5) * 1000)),
              "--end-time", str(int((t_call + 180) * 1000)),
              "--query", "events[].[timestamp,message]", "--output", "json")
    ev = []
    for ts, m in json.loads(out) or []:
        if KEEP.search(m) and not m.startswith(("\tat", "Caused")):
            msg = re.sub(r"\s+", " ", re.sub(r"^\S+ \S+\s+", "", m.split("\n")[0]))
            msg = re.sub(r"SnapshotResult \[status=(\w+).*", r"SnapshotResult status=\1", msg)
            ev.append({"s": round(ts / 1000 - t_call, 1), "debezium": re.sub(r"\s*\[io\.debezium[^\]]*\]", "", msg)[:140]})
    first = round(min(e[0] for e in errors) - t_call, 2) if errors else None
    noticed = next((e["s"] for e in ev if "Producer failure" in e["debezium"]), None)
    return {"first_writer_error_s": first, "debezium_noticed_s": noticed, "events": ev}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=240)
    ap.add_argument("--no-failover", action="store_true")
    ap.add_argument("--crash", action="store_true", help="crash the writer instead of a requested failover")
    ap.add_argument("--rate", type=int, default=40, help="inserts per second")
    ap.add_argument("--lead", type=int, default=45, help="seconds of writes before the failover")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    t = Ssm()
    run_id = "soak-" + time.strftime("%Y%m%d-%H%M%S")

    before = {s: status(t, s) for s in "ab"}
    if before["a"]["connector"] != "RUNNING/RUNNING":
        raise SystemExit(f"connector not healthy before the test: {before['a']['connector']}")
    rc, out, err = run(t, "a", ["soak-prepare", run_id, str(a.rate)])
    if rc != 0:
        raise SystemExit(f"soak-prepare failed: {err or out}")
    hb = t.start("b", ["soak-watch", run_id])
    ha = t.start("a", ["soak-run", run_id, str(a.duration)])
    deadline = time.time() + 240
    while True:     # `started` is published once the first write is about to go out
        try:
            t.fetch(f"{run_id}/started")
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("writer never started")
            time.sleep(3)
    print("writes are flowing", flush=True)

    fo: dict = {}
    if a.crash:
        time.sleep(a.lead)
        fo = {"mode": "crash", "old_writer": writer(t.n), "t_call": time.time()}
        rc, out, err = run(t, "a", ["crash-writer"])
        print(f"crash: rc={rc} {out.strip()}", flush=True)
        if rc != 0:
            fo["crash_failed"] = (err or out)[-300:]
    elif not a.no_failover:
        time.sleep(a.lead)
        old = writer(t.n)
        fo = {"mode": "failover", "t_call": time.time(), "old_writer": old}
        aws(t.n["region_a"], "rds", "failover-db-cluster", "--db-cluster-identifier", t.n["cluster_id"])
        print(f"failover requested (writer {old})", flush=True)
        while (now := writer(t.n)) == old:
            if time.time() - fo["t_call"] > 300:
                break
            time.sleep(2)
        fo |= {"new_writer": now, "api_flip_s": round(time.time() - fo["t_call"], 1)}
        print(f"writer is now {now} after {fo['api_flip_s']}s (API view)", flush=True)

    (rc_a, out_a, err_a), (rc_b, out_b, err_b) = t.wait(ha), t.wait(hb)
    for s, rc, out, err in (("a", rc_a, out_a, err_a), ("b", rc_b, out_b, err_b)):
        print(f"[{s}] rc={rc} {out.strip()[-400:]}" + (f"\n{err[-1500:]}" if rc else ""), flush=True)
    after = {s: status(t, s) for s in "ab"}
    if a.crash:
        fo["new_writer"] = writer(t.n)

    stages = {}
    rc, out, err = run(t, "a", ["soak-stages", run_id])
    if rc == 0:
        stages = json.loads(out.strip().splitlines()[-1])
    else:
        print(f"stage probe failed (not part of the verdict): {err[-400:]}", file=sys.stderr)
    w = json.loads(t.fetch(f"{run_id}/writes.json"))
    sides = {s: json.loads(t.fetch(f"{run_id}/watch-{s}.json")) for s in "ab"}
    summary = soak_summary(w, sides)
    ok, problems = soak_verdict(summary, before, after, fo or None)
    if fo:
        fo["timeline"] = timeline(t.n, fo["t_call"], w["errors"])
    if rc_a or rc_b:
        ok = False
        problems.append(f"node exit codes a={rc_a} b={rc_b}")
    result = {"run": run_id, "ok": ok, "problems": problems, "failover": fo,
              "before": before, "after": after, "summary": summary, "stages": stages}
    print(json.dumps(summary, indent=2))
    if stages:
        print("stages:", json.dumps(stages, indent=1))
    for p in problems:
        print("FAIL:", p)
    print("RESULT:", "PASS" if ok else "FAIL")
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"saved {os.fspath(a.out)}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
