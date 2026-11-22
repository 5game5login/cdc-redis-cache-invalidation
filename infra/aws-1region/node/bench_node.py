"""Node-side benchmark steps. Run inside the `tools` container; I/O is /run/cdc/<run>.

  prepare  insert the test keys                         -> plan.json
  write    fire concurrent writes                       -> writes.json, truth.json
  observe  poll the local cache for first-seen versions -> observe-<side>.json

Lag = first seen in cache - commit acknowledged to the writer. Both ends use the
host clock (chrony + Amazon Time Sync); the measured clock offset is saved with
the results. Polling adds up to one poll interval (about 5 ms) of upward bias.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bench_lib import mismatches

N_KEYS = int(os.getenv("BENCH_KEYS", "50"))
N_OPS = int(os.getenv("BENCH_OPS", "500"))
N_WRITERS = int(os.getenv("BENCH_WRITERS", "25"))
SPREAD_S = float(os.getenv("BENCH_SPREAD_S", "2"))
P_DELETE, P_REINSERT = 0.10, 0.10
POLL_S = 0.005


def write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)      # atomic: readers never see a partial file


def prepare(d: Path) -> None:
    import psycopg
    tag = f"bench-{uuid.uuid4().hex[:6]}"
    with psycopg.connect(os.environ["APP_DSN"], autocommit=True) as c:
        ids = [r[0] for r in c.execute(
            "INSERT INTO products (name, price, stock) "
            "SELECT %s || '-' || g, 10.00, 0 FROM generate_series(1, %s) g RETURNING id",
            (tag, N_KEYS)).fetchall()]
    write_json(d / "plan.json", {"tag": tag, "ids": ids})
    print(f"prepared {len(ids)} keys tag={tag}")


def write(d: Path) -> None:
    import psycopg
    plan = json.loads((d / "plan.json").read_text())
    ids, tag = plan["ids"], plan["tag"]
    rng = random.Random(int(os.getenv("BENCH_SEED", str(int(time.time())))))
    reinsert_stock = itertools.count(1_000_000)
    picks = [rng.choice(ids) for _ in range(N_OPS)]
    chunks = [picks[i::N_WRITERS] for i in range(N_WRITERS)]
    commits: list[list[float]] = []
    ops = {"update": 0, "delete": 0, "reinsert": 0, "noop": 0}
    lock = threading.Lock()
    barrier = threading.Barrier(N_WRITERS)

    def update(c, pk):
        return c.execute(
            "UPDATE products SET stock = stock + 1, price = round((random() * 100)::numeric, 2), "
            "updated_at = clock_timestamp() WHERE id = %s RETURNING stock", (pk,)).fetchone()

    def reinsert(c, pk):
        return c.execute(
            "INSERT INTO products (id, name, price, stock) VALUES (%s, %s, 10.00, %s) "
            "ON CONFLICT (id) DO NOTHING RETURNING stock",
            (pk, f"{tag}-re", next(reinsert_stock))).fetchone()

    def apply_op(c, pk, x):
        """Every op commits a real change: an op that cannot apply to the key's
        current state (e.g. updating a deleted row) falls back to one that can."""
        if x < P_DELETE and c.execute("DELETE FROM products WHERE id = %s", (pk,)).rowcount:
            return "delete", None
        first, second = (("reinsert", reinsert), ("update", update))
        if x >= P_DELETE + P_REINSERT:
            first, second = second, first
        for _ in range(3):      # bounded: another writer may flip the key in between
            for kind, fn in (first, second):
                if row := fn(c, pk):
                    return kind, row
        return "noop", None

    def writer(keys: list[int], r: random.Random) -> None:
        with psycopg.connect(os.environ["APP_DSN"], autocommit=True) as c:
            barrier.wait(60)    # connection setup is not part of the workload
            for pk in keys:
                time.sleep(r.random() * 2 * SPREAD_S / max(1, len(keys)))
                kind, row = apply_op(c, pk, r.random())
                ts = time.time()
                with lock:
                    ops[kind] += 1
                    if row:
                        commits.append([pk, row[0], ts])

    t0 = time.time()
    with ThreadPoolExecutor(N_WRITERS) as pool:
        list(pool.map(writer, chunks, [random.Random(rng.random()) for _ in chunks]))
    t1 = time.time()
    with psycopg.connect(os.environ["APP_DSN"], autocommit=True) as c:
        rows = {r[0]: {"name": r[1], "price": str(r[2]), "stock": r[3]} for r in c.execute(
            "SELECT id, name, price, stock FROM products WHERE id = ANY(%s)", (ids,))}
    write_json(d / "writes.json", {"commits": commits, "op_counts": ops, "duration_s": round(t1 - t0, 2)})
    write_json(d / "truth.json", {"t_end": t1, "rows": {str(pk): rows.get(pk) for pk in ids}})
    print(f"writes done in {t1 - t0:.1f}s: {ops}")


def observe(d: Path, side: str, timeout_s: float) -> int:
    from worker.invalidator import Invalidator
    plan = json.loads((d / "plan.json").read_text())
    ids = plan["ids"]
    inv = Invalidator()
    seen: dict[tuple[int, int], float] = {}

    def poll() -> dict[str, dict]:
        pipe = inv.r.pipeline(transaction=False)
        for pk in ids:
            pipe.hmget(inv.key(pk), "lsn", "deleted", "payload")
        res = pipe.execute()
        now = time.time()
        out: dict[str, dict] = {}
        for pk, (lsn, deleted, payload) in zip(ids, res):
            if lsn is None:
                continue
            row = json.loads(payload) if payload else None
            out[str(pk)] = {"deleted": deleted == "1", "payload": row}
            if row and deleted != "1":
                seen.setdefault((pk, row["stock"]), now)
        return out

    poll()
    (d / f"ready-{side}").write_text("ok")
    deadline = time.time() + timeout_s
    truth, bad, converge_s = None, [str(i) for i in ids], None
    while time.time() < deadline:
        cache = poll()
        if truth is None and (d / "truth.json").exists():
            truth = json.loads((d / "truth.json").read_text())
        if truth is not None:
            bad = mismatches(truth["rows"], cache)
            if not bad:
                converge_s = round(max(0.0, time.time() - truth["t_end"]), 2)
                break
        time.sleep(POLL_S)
    if truth is not None and not bad:
        time.sleep(2)     # late events must not break consistency
        bad = mismatches(truth["rows"], poll())
    write_json(d / f"observe-{side}.json", {
        "seen": [[pk, stock, ts] for (pk, stock), ts in seen.items()],
        "total": len(ids), "mismatched": bad, "converge_s": converge_s})
    print(f"observe {side}: {len(ids) - len(bad)}/{len(ids)} consistent, {len(seen)} versions seen")
    return 0 if not bad else 1


def slots() -> None:
    import psycopg
    with psycopg.connect(os.environ["SLOT_ADMIN_DSN"], autocommit=True, connect_timeout=10) as c:
        rows = c.execute("SELECT slot_name, active, wal_status, confirmed_flush_lsn::text "
                         "FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY 1").fetchall()
    print(json.dumps([dict(zip(("slot", "active", "wal_status", "confirmed_flush_lsn"), r)) for r in rows]))


def drop_slot(name: str) -> None:
    import psycopg
    with psycopg.connect(os.environ["SLOT_ADMIN_DSN"], autocommit=True, connect_timeout=10) as c:
        for _ in range(30):
            row = c.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (name,)).fetchone()
            if row is None:
                print(f"slot {name} absent")
                return
            if not row[0]:
                c.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                print(f"dropped slot {name}")
                return
            time.sleep(2)
    raise SystemExit(f"slot {name} is still active; refusing to drop it")


def crash_writer() -> None:
    """Aurora fault injection: crash the writer's database process (no clean shutdown)."""
    import psycopg
    with psycopg.connect(os.environ["ADMIN_DSN"], autocommit=True, connect_timeout=10) as c:
        try:
            c.execute("SELECT aurora_inject_crash('instance')")
        except psycopg.OperationalError as e:
            print(f"writer crashed: {type(e).__name__}")    # expected: our session died with it
            return
    raise SystemExit("aurora_inject_crash returned without crashing the writer")


def epoch() -> None:
    from worker.invalidator import EPOCH_KEY, Invalidator
    print(Invalidator().r.get(EPOCH_KEY))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["prepare", "write", "observe", "slots", "drop-slot", "epoch", "crash-writer"])
    ap.add_argument("--run")
    ap.add_argument("--side")
    ap.add_argument("--slot")
    ap.add_argument("--timeout", type=float, default=180)
    a = ap.parse_args()
    if a.cmd == "slots":
        slots()
    elif a.cmd == "drop-slot":
        drop_slot(a.slot)
    elif a.cmd == "epoch":
        epoch()
    elif a.cmd == "crash-writer":
        crash_writer()
    else:
        d = Path("/run/cdc") / a.run
        if a.cmd == "prepare":
            prepare(d)
        elif a.cmd == "write":
            write(d)
        else:
            return observe(d, a.side, a.timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
