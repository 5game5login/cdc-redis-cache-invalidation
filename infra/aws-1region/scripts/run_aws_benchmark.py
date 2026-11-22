"""Benchmark both caches: 500 concurrent writes to Aurora, then p50/p99
invalidation lag and 100% convergence, measured independently on each side.

Run from the operator machine (no inbound access needed: everything goes through
SSM and the results bucket). Exits 0 only if both sides fully converge.

    python infra/aws-1region/scripts/run_aws_benchmark.py [--out results/aws/x.json]
    CDC_LOCAL=<dir> python ... run_aws_benchmark.py      # local two-sided stack
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / "node")]

from bench_lib import summarize, verdict  # noqa: E402
from cdc_remote import run, transport  # noqa: E402


def benchmark(t, run_id: str) -> tuple[dict, bool]:
    rc, out, err = run(t, "a", ["bench-prepare", run_id])
    if rc != 0:
        raise SystemExit(f"bench-prepare failed (rc={rc}): {err or out}")
    hb = t.start("b", ["bench-observe", run_id])
    ha = t.start("a", ["bench-run", run_id])
    (rc_a, out_a, err_a), (rc_b, out_b, err_b) = t.wait(ha), t.wait(hb)
    for side, rc, out, err in (("a", rc_a, out_a, err_a), ("b", rc_b, out_b, err_b)):
        if rc != 0:
            print(f"side {side} rc={rc}\n{out}\n{err}", file=sys.stderr)

    def load(name: str):
        return json.loads(t.fetch(f"{run_id}/{name}"))

    writes = load("writes.json")
    summaries = []
    for side in ("a", "b"):
        try:
            summaries.append(summarize(side, writes["commits"], load(f"observe-{side}.json")))
        except Exception as e:
            print(f"no result from side {side}: {e}", file=sys.stderr)
    ok, problems = verdict(summaries, writes)
    if len(summaries) != 2 or rc_a or rc_b:
        ok = False
        problems.append(f"incomplete run (rc a={rc_a} b={rc_b}, sides={len(summaries)})")
    return {"run": run_id, "ok": ok, "problems": problems, "ops": writes["op_counts"],
            "write_duration_s": writes["duration_s"], "sides": summaries}, ok


def show(r: dict) -> None:
    print(f"run {r['run']}: ops={r['ops']} in {r['write_duration_s']}s")
    for s in r["sides"]:
        print(f"  side {s['side']}: {s['consistent']}/{s['keys']} consistent, "
              f"converged {s['converge_after_last_write_s']}s after last write, "
              f"lag n={s['samples']} p50={s['p50_ms']}ms p99={s['p99_ms']}ms max={s['max_ms']}ms"
              + (f"  [clock {s['clock']}]" if s["clock"] else ""))
    for p in r["problems"]:
        print(f"  FAIL: {p}")
    print("RESULT:", "PASS" if r["ok"] else "FAIL")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path)
    ap.add_argument("--repeat", type=int, default=1)
    a = ap.parse_args()
    t = transport()
    results, all_ok = [], True
    for i in range(a.repeat):
        r, ok = benchmark(t, time.strftime("%Y%m%d-%H%M%S") + (f"-{i}" if a.repeat > 1 else ""))
        show(r)
        results.append(r)
        all_ok &= ok
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"saved {a.out}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
