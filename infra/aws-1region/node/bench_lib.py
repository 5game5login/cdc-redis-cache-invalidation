"""Pure benchmark logic (no I/O), unit-tested in tests/test_aws_bench.py."""
from __future__ import annotations

from typing import Any

Key = tuple[int, int]      # (product id, stock): unique per committed write


def pctl(data: list[float], p: float) -> float:
    if not data:
        return float("nan")
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def join_lags(commits: list[list[float]], seen: list[list[float]]) -> list[float]:
    """Milliseconds from commit to first sight in the cache, per observed write.

    Coalesced versions never reach the cache and so contribute no sample."""
    t0: dict[Key, float] = {(int(pk), int(stock)): ts for pk, stock, ts in commits}
    return [(ts - t0[(int(pk), int(stock))]) * 1000
            for pk, stock, ts in seen if (int(pk), int(stock)) in t0]


def entry_matches(want: dict[str, Any] | None, entry: dict[str, Any] | None) -> bool:
    """`entry` is {"deleted": bool, "payload": dict | None}, or None if absent."""
    if want is None:
        return entry is None or bool(entry["deleted"])
    if entry is None or entry["deleted"] or not entry["payload"]:
        return False
    return all(entry["payload"].get(k) == v for k, v in want.items())


def mismatches(truth: dict[str, Any], cache: dict[str, Any]) -> list[str]:
    return [pk for pk, want in truth.items() if not entry_matches(want, cache.get(pk))]


def summarize(side: str, commits: list[list[float]], obs: dict[str, Any]) -> dict[str, Any]:
    lags = join_lags(commits, obs["seen"])
    return {
        "side": side,
        "keys": obs["total"],
        "consistent": obs["total"] - len(obs["mismatched"]),
        "mismatched": obs["mismatched"][:5],
        "converge_after_last_write_s": obs["converge_s"],
        "samples": len(lags),
        "p50_ms": round(pctl(lags, 50), 1) if lags else None,
        "p99_ms": round(pctl(lags, 99), 1) if lags else None,
        "min_ms": round(min(lags), 1) if lags else None,
        "max_ms": round(max(lags), 1) if lags else None,
        "clock": obs.get("clock", ""),
    }


def verdict(summaries: list[dict[str, Any]], writes: dict[str, Any]) -> tuple[bool, list[str]]:
    """PASS only if every side fully converged and the workload was really exercised."""
    problems: list[str] = []
    if not summaries:
        problems.append("no observations")
    for s in summaries:
        if s["keys"] == 0:
            problems.append(f"{s['side']}: no keys checked")
        if s["consistent"] != s["keys"]:
            problems.append(f"{s['side']}: {s['keys'] - s['consistent']}/{s['keys']} keys inconsistent")
        if s["samples"] == 0:
            problems.append(f"{s['side']}: no lag samples")
        elif s["min_ms"] < 0:
            problems.append(f"{s['side']}: negative lag {s['min_ms']} ms (clock skew or commit-time ordering)")
    for op in ("update", "delete", "reinsert"):
        if not writes["op_counts"].get(op):
            problems.append(f"workload produced no {op}s")
    return not problems, problems


# ---------------------------------------------------------------- failover soak
def followup(n: int) -> str | None:
    """Deterministic later event for key n: 25% update, 15% delete, 60% none."""
    k = n % 20
    return "update" if k < 5 else "delete" if k < 8 else None


def longest_gap(times: list[float]) -> tuple[float, float]:
    """(seconds, start time) of the longest pause between consecutive events."""
    ts = sorted(times)
    gaps = [(b - a, a) for a, b in zip(ts, ts[1:])]
    return max(gaps) if gaps else (0.0, 0.0)


def bursts(times: list[float], min_gap: float = 0.05) -> dict[str, Any]:
    ts = sorted(times)
    gaps = [b - a for a, b in zip(ts, ts[1:]) if b - a > min_gap]
    return {"pauses": len(gaps), "median_pause_ms": round(pctl(gaps, 50) * 1000, 1) if gaps else 0.0}


def soak_summary(w: dict[str, Any], sides: dict[str, dict[str, Any]]) -> dict[str, Any]:
    gap, _ = longest_gap(w["acks"])
    out: dict[str, Any] = {
        "keys": w["keys"], "acked_commits": len(w["acks"]), "acked_inserts": len(w["inserts"]),
        "write_errors": len(w["errors"]), "gave_up": len(w["failed"]),
        "db_missing": len(w["db_missing"]), "longest_write_pause_s": round(gap, 2),
        "duration_s": round(w["t_end"] - w["t0"], 1),
    }
    if w["errors"]:
        out["error_window_s"] = round(max(e[0] for e in w["errors"]) - min(e[0] for e in w["errors"]), 2)
    ack = {int(i): ts for i, _, ts in w["inserts"]}
    for side, v in sides.items():
        lags = [ts - ack[int(i)] for i, _, ts in v["seen"] if int(i) in ack]
        pause, _ = longest_gap([ts for i, _, ts in v["seen"] if int(i) in ack])
        out[side] = {
            "keys": v["total"], "mismatched": len(v["mismatched"]), "absent": len(v["missing"]),
            "mismatched_sample": v["mismatched"][:10], "converge_after_last_write_s": v["converge_s"],
            "lag_samples": len(lags),
            "lag_p50_ms": round(pctl(lags, 50) * 1000, 1) if lags else None,
            "lag_p99_ms": round(pctl(lags, 99) * 1000, 1) if lags else None,
            "lag_max_s": round(max(lags), 2) if lags else None,
            "longest_cache_pause_s": round(pause, 2),
        }
    return out


def soak_verdict(s: dict[str, Any], before: dict, after: dict, failover: dict | None) -> tuple[bool, list[str]]:
    p: list[str] = []
    if s["keys"] == 0 or s["acked_inserts"] != s["keys"]:
        p.append(f"only {s['acked_inserts']} of {s['keys']} inserts were acknowledged")
    if s["gave_up"]:
        p.append(f"{s['gave_up']} operations gave up")
    if s["db_missing"]:
        p.append(f"{s['db_missing']} acknowledged inserts are missing from Aurora")
    for side in "ab":
        if side not in s:
            p.append(f"no result from side {side}")
            continue
        if s[side]["keys"] != s["keys"]:
            p.append(f"{side}: verified {s[side]['keys']} of {s['keys']} keys")
        if s[side]["mismatched"]:
            p.append(f"{side}: {s[side]['mismatched']} keys differ from Aurora ({s[side]['absent']} absent)")
        for f in ("cache_epoch", "topic", "slot"):
            if before[side][f] != after[side][f]:
                p.append(f"{side}: {f} changed {before[side][f]} -> {after[side][f]}")
    if after["a"]["connector"] != "RUNNING/RUNNING":
        p.append(f"connector is {after['a']['connector']}")
    if failover is not None:
        if failover.get("crash_failed"):
            p.append(f"crash was not injected: {failover['crash_failed']}")
        if failover.get("mode") != "crash" and failover.get("old_writer") == failover.get("new_writer"):
            p.append("the writer did not change")
        if s["write_errors"] == 0:
            p.append("no write was interrupted: the failover did not overlap the workload")
    return not p, p
