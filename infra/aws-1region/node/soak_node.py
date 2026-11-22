"""Continuous-write loss test, meant to run across an Aurora failover.

  prepare  choose the key range                           -> plan.json
  write    paced writes that survive connection loss      -> writes.json, truth.json
  watch    first-seen times, then verify every key        -> watch-<side>.json

Why a lost event cannot hide: each key is inserted once, and at most one later
event (update or delete) follows. The final event of every key is therefore checked
individually. (A lost insert that is followed by an update/delete is masked, but then
the cache is still correct.) Ids are fixed in advance and every operation is
idempotent, so an operation whose commit outcome is unknown is simply retried.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

from bench_lib import followup, mismatches
from bench_node import write_json

MAX_KEYS = 99_000           # ranges are 100k apart and stay inside a 32-bit id


def prepare(d: Path, rate: float) -> None:
    base = 100_000_000 + (int(time.time()) % 20_000) * 100_000
    write_json(d / "plan.json", {"base": base, "max": MAX_KEYS, "tag": f"soak-{base}", "rate": rate})
    print(f"soak key range starts at {base}, {rate:g} inserts/s")


def write(d: Path, duration_s: float) -> None:
    import psycopg
    plan = json.loads((d / "plan.json").read_text())
    base, tag, rate = plan["base"], plan["tag"], plan["rate"]
    n_threads = max(4, int(rate // 10))
    distance = max(120, int(rate * 3))      # a follow-up runs about 3 s after its insert
    lock = threading.Lock()
    inserts: list[list[float]] = []     # [id, stock, ack time]
    acks: list[float] = []              # every acknowledged commit
    errors: list[list] = []             # [time, type]
    failed: list[str] = []
    t0 = time.time()
    next_n = [0]

    def connect():
        # read-write: right after a failover DNS may still name the old writer
        return psycopg.connect(os.environ["APP_DSN"], autocommit=True, connect_timeout=3,
                               tcp_user_timeout=5000, target_session_attrs="read-write",
                               options="-c statement_timeout=5000")

    def worker() -> None:
        conn = None

        def run(sql: str, args: tuple) -> bool:
            nonlocal conn
            deadline = time.time() + 180
            while time.time() < deadline:
                try:
                    if conn is None:
                        conn = connect()
                    conn.execute(sql, args)
                    with lock:
                        acks.append(time.time())
                    return True
                except psycopg.Error as e:
                    with lock:
                        errors.append([time.time(), type(e).__name__])
                    try:
                        if conn is not None:
                            conn.close()
                    finally:
                        conn = None
                    time.sleep(0.2)
            with lock:
                failed.append(sql.split()[0])
            return False

        while True:
            with lock:
                n = next_n[0]
                if n >= MAX_KEYS or time.time() - t0 > duration_s:
                    break
                next_n[0] += 1
            delay = t0 + n / rate - time.time()
            if delay > 0:
                time.sleep(delay)
            if run("INSERT INTO products (id, name, price, stock) VALUES (%s, %s, 10.00, %s) "
                   "ON CONFLICT (id) DO NOTHING", (base + n, f"{tag}-{n}", n)):
                with lock:
                    inserts.append([base + n, n, time.time()])
            m = n - distance
            if m >= 0:
                kind = followup(m)
                if kind == "update":
                    run("UPDATE products SET stock = %s, price = 77.00 WHERE id = %s", (1_000_000 + m, base + m))
                elif kind == "delete":
                    run("DELETE FROM products WHERE id = %s", (base + m,))
        if conn is not None:
            conn.close()

    (d / "started").write_text("ok")
    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t_end = time.time()
    n_keys = next_n[0]
    with connect() as c:
        rows = {r[0]: {"name": r[1], "price": str(r[2]), "stock": r[3]} for r in c.execute(
            "SELECT id, name, price, stock FROM products WHERE id >= %s AND id < %s",
            (base, base + MAX_KEYS))}
    # Durability: an acknowledged insert must be in Aurora unless we deleted it later
    deleted = {base + m for m in range(max(0, n_keys - distance)) if followup(m) == "delete"}
    db_missing = sorted(int(i) for i, _, _ in inserts if i not in rows and i not in deleted)
    write_json(d / "writes.json", {
        "inserts": inserts, "acks": sorted(acks), "errors": errors, "failed": failed,
        "db_missing": db_missing, "keys": n_keys, "t0": t0, "t_end": t_end})
    write_json(d / "truth.json", {"t_end": t_end, "rows": {str(base + n): rows.get(base + n) for n in range(n_keys)}})
    print(f"soak: {n_keys} keys, {len(acks)} acked commits, {len(errors)} errors, "
          f"{len(failed)} gave up, {len(db_missing)} acked inserts missing from Aurora")


def watch(d: Path, side: str, timeout_s: float) -> int:
    from worker.invalidator import Invalidator
    plan = json.loads((d / "plan.json").read_text())
    base, top = plan["base"], plan["base"] + plan["max"]
    window = max(400, int(plan["rate"] * 10))
    inv = Invalidator()
    seen: dict[int, float] = {}
    pending: set[int] = set()
    hi = base
    (d / f"ready-{side}").write_text("ok")
    deadline = time.time() + timeout_s
    truth = None

    def read_cache(keys: list[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for i in range(0, len(keys), 1000):
            part = keys[i:i + 1000]
            pipe = inv.r.pipeline(transaction=False)
            for k in part:
                pipe.hmget(inv.key(k), "lsn", "deleted", "payload")
            for k, (lsn, deleted, payload) in zip(part, pipe.execute()):
                if lsn is not None:
                    out[k] = {"deleted": deleted == "1", "payload": json.loads(payload) if payload else None}
        return out

    bad, missing, converge_s = [], [], None
    while time.time() < deadline:
        while hi < top and len(pending) < window:      # sliding window of ids not seen yet
            pending.add(hi)
            hi += 1
        ids = sorted(pending)
        pipe = inv.r.pipeline(transaction=False)
        for i in ids:
            pipe.exists(inv.key(i))
        res = pipe.execute()
        now = time.time()
        for i, ok in zip(ids, res):
            if ok:
                seen[i] = now
                pending.discard(i)
        if truth is None and (d / "truth.json").exists():
            truth = json.loads((d / "truth.json").read_text())
        if truth is not None:
            cache = read_cache(list(truth["rows"]))
            bad = mismatches(truth["rows"], cache)
            if not bad:
                converge_s = round(max(0.0, time.time() - truth["t_end"]), 2)
                break
            time.sleep(0.5)
        time.sleep(0.02)
    if truth is not None:
        if not bad:
            time.sleep(5)       # late replays must not break consistency
        cache = read_cache(list(truth["rows"]))
        bad = mismatches(truth["rows"], cache)
        missing = [k for k in bad if k not in cache]
    total = len(truth["rows"]) if truth else 0
    write_json(d / f"watch-{side}.json", {
        "seen": [[i, i - base, t] for i, t in seen.items()], "total": total,
        "mismatched": bad if truth else ["no truth"], "missing": missing, "converge_s": converge_s})
    print(f"watch {side}: {total - len(bad)}/{total} consistent, {len(missing)} absent, {len(seen)} keys seen")
    return 0 if truth is not None and not bad else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["prepare", "write", "watch"])
    ap.add_argument("--run", required=True)
    ap.add_argument("--side")
    ap.add_argument("--duration", type=float, default=240)
    ap.add_argument("--timeout", type=float, default=700)
    ap.add_argument("--rate", type=float, default=40)
    a = ap.parse_args()
    d = Path("/run/cdc") / a.run
    if a.cmd == "prepare":
        prepare(d, a.rate)
    elif a.cmd == "write":
        write(d, a.duration)
    else:
        return watch(d, a.side, a.timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
