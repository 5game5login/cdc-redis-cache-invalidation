"""Chaos & concurrency verification against the live stack.

Two phases, each on 50 fresh keys, each asserting 100% PostgreSQL == Redis:

  steady  500 concurrent writes (updates, deletes, re-inserts) through two
          competing workers whose input is reordered, duplicated and
          replayed late. Reports p50/p99 invalidation lag.

  faults  the same workload, plus, while writes are in flight:
            - a worker crashes after writing Redis but before committing
              offsets (its batch is redelivered to another worker)
            - a new worker joins (rebalance) and another leaves (rebalance)
            - Redis is frozen (docker pause) longer than the socket timeout
            - the Debezium connector is restarted (re-emits recent events)

Lag is host commit time -> Redis apply time (same clock).

Run:  python tests/chaos_test.py [--phase steady|faults|all]
                                 [--no-redis-outage] [--no-connector-restart]
Exits 0 on success, 1 on failure.
Requires: docker compose up -d && bash debezium/register-connector.sh
"""
from __future__ import annotations

import argparse
import itertools
import logging
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg  # noqa: E402
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from worker.consumer import ChangeEvent, InvalidationWorker, Record, Settings  # noqa: E402

N_KEYS = int(os.getenv("CHAOS_KEYS", "50"))
N_OPS = int(os.getenv("CHAOS_OPS", "500"))
N_WRITERS = int(os.getenv("CHAOS_WRITERS", "25"))
P_DELETE, P_REINSERT = 0.10, 0.10   # remaining ops are updates
P_DUPLICATE = 0.30                  # emit an extra copy immediately
P_DELAY = 0.25                      # also replay a copy several batches later
REDIS_PAUSE_S = 7.0                 # > worker socket timeout (5s) so retries trigger
CONVERGE_TIMEOUT_S = 120
# Writer/test DSN needs write access; the worker itself never writes to PG.
ADMIN_DSN = os.getenv("CHAOS_PG_DSN", "postgresql://postgres:postgres@127.0.0.1:5432/inventory")
CONNECT_URL = os.getenv("CONNECT_URL", "http://127.0.0.1:8083")
CONNECTOR = os.getenv("CONNECTOR_NAME", "products-connector")
SEED = int(os.getenv("CHAOS_SEED", str(int(time.time()))))


class SimulatedCrash(RuntimeError):
    pass


class ChaosInterceptor:
    """Reorders, duplicates and late-replays records on their way into a worker."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.held: list[tuple[int, Record]] = []
        self.tick = 0
        self.duplicated = self.delayed = 0

    def __call__(self, records: list[Record]) -> list[Record]:
        self.tick += 1
        out: list[Record] = []
        for rec in records:
            out.append(rec)
            if self.rng.random() < P_DUPLICATE:
                out.append(rec)
                self.duplicated += 1
            if self.rng.random() < P_DELAY:
                self.held.append((self.tick + self.rng.randint(2, 15), rec))
                self.delayed += 1
        self.rng.shuffle(out)  # breaks per-key ordering within the batch
        due = [r for t, r in self.held if t <= self.tick]
        self.held = [(t, r) for t, r in self.held if t > self.tick]
        return out + due      # late replays land after newer versions


class ChaosWorker(InvalidationWorker):
    """Worker that can be told to crash between the Redis write and the offset commit."""

    def __init__(self, name: str, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.name = name
        self.crash_requested = threading.Event()
        self.crashed = threading.Event()
        self.assignments = 0
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)

    def _run(self) -> None:
        try:
            self.run()
        except SimulatedCrash:
            pass

    def _commit(self, offsets) -> None:
        if self.crash_requested.is_set():
            self.crashed.set()
            raise SimulatedCrash(f"{self.name}: crashed after Redis write, before offset commit")
        super()._commit(offsets)


@dataclass
class Phase:
    name: str
    faults: bool
    lags_ms: list[float] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    t0: float = 0.0

    def log(self, msg: str) -> None:
        line = f"  [{time.time() - self.t0:5.1f}s] {msg}"
        self.events.append(line)
        print(line, flush=True)


def pctl(data: list[float], p: float) -> float:
    if not data:
        return float("nan")
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def docker_available() -> bool:
    try:
        return subprocess.run(["docker", "inspect", "redis"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def restart_connector() -> int:
    req = urllib.request.Request(f"{CONNECT_URL}/connectors/{CONNECTOR}/restart?includeTasks=true", method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def pin_group_to_end(settings: Settings) -> int:
    """Commit the current end offsets for a fresh consumer group, so its workers
    consume exactly this phase's events while still using normal group offsets
    (crash/rebalance redelivery then behaves as in production)."""
    c = Consumer({"bootstrap.servers": settings.bootstrap, "group.id": settings.group_id,
                  "enable.auto.commit": False})
    parts = c.list_topics(settings.topic, timeout=10).topics[settings.topic].partitions
    tps = []
    for pid in parts:
        _, high = c.get_watermark_offsets(TopicPartition(settings.topic, pid), timeout=10)
        tps.append(TopicPartition(settings.topic, pid, high))
    c.commit(offsets=tps, asynchronous=False)
    c.close()
    return len(parts)


def run_phase(phase: Phase, rng: random.Random, redis_outage: bool, connector_restart: bool) -> bool:
    print(f"\n=== phase: {phase.name} ===", flush=True)
    settings = Settings(group_id=f"chaos-{phase.name}-{uuid.uuid4().hex[:8]}", batch_size=100)
    n_parts = pin_group_to_end(settings)
    tag = f"chaos-{phase.name}-{uuid.uuid4().hex[:6]}"

    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        ids = [r[0] for r in conn.execute(
            "INSERT INTO products (name, price, stock) "
            "SELECT %s || '-' || g, 10.00, 0 FROM generate_series(1, %s) g RETURNING id",
            (tag, N_KEYS)).fetchall()]

    commit_times: dict[tuple[int, int], float] = {}   # (id, stock) -> host commit time
    lag_lock = threading.Lock()

    def on_applied(ev: ChangeEvent) -> None:
        if ev.row is None:
            return
        t0 = commit_times.get((ev.pk, ev.row.get("stock")))
        if t0 is not None:
            with lag_lock:
                phase.lags_ms.append((time.time() - t0) * 1000)

    interceptors: list[ChaosInterceptor] = []

    def spawn(name: str) -> ChaosWorker:
        ic = ChaosInterceptor(random.Random(rng.random()))
        interceptors.append(ic)
        w = ChaosWorker(name, settings, interceptor=ic, on_applied=on_applied)
        w.thread.start()
        return w

    workers = [spawn("w1"), spawn("w2")]
    for w in workers:
        if not w.ready.wait(30):
            print(f"FAIL: {w.name} never got a partition assignment")
            return False
    phase.t0 = time.time()
    phase.log(f"{len(workers)} workers consuming {n_parts} partitions; {N_KEYS} keys; tag={tag}")

    # ---- workload: updates / deletes / re-inserts, spread over a few seconds
    reinsert_stock = itertools.count(1_000_000)  # unique stock values keep lag keys unambiguous
    plan = [rng.choice(ids) for _ in range(N_OPS)]
    chunks = [plan[i::N_WRITERS] for i in range(N_WRITERS)]
    op_counts = {"update": 0, "delete": 0, "reinsert": 0, "noop": 0}
    op_lock = threading.Lock()
    spread_s = 6.0 if phase.faults else 2.0

    def writer(keys: list[int], wrng: random.Random) -> None:
        with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
            for pk in keys:
                time.sleep(wrng.random() * 2 * spread_s / max(1, len(keys)))
                x = wrng.random()
                if x < P_DELETE:
                    n = conn.execute("DELETE FROM products WHERE id = %s", (pk,)).rowcount
                    kind = "delete" if n else "noop"
                elif x < P_DELETE + P_REINSERT:
                    stock = next(reinsert_stock)
                    row = conn.execute(
                        "INSERT INTO products (id, name, price, stock) VALUES (%s, %s, 10.00, %s) "
                        "ON CONFLICT (id) DO NOTHING RETURNING stock", (pk, f"{tag}-re", stock)).fetchone()
                    kind = "reinsert" if row else "noop"
                    if row:
                        commit_times[(pk, row[0])] = time.time()
                else:
                    row = conn.execute(
                        "UPDATE products SET stock = stock + 1, price = round((random() * 100)::numeric, 2), "
                        "updated_at = clock_timestamp() WHERE id = %s RETURNING stock", (pk,)).fetchone()
                    kind = "update" if row else "noop"
                    if row:
                        commit_times[(pk, row[0])] = time.time()
                with op_lock:
                    op_counts[kind] += 1

    # ---- fault schedule (runs concurrently with the writers)
    fault_errors: list[str] = []
    crashed_worker: list[ChaosWorker] = []

    def faults() -> None:
        try:
            time.sleep(1.0)
            victim = workers[0]
            victim.crash_requested.set()
            if victim.crashed.wait(15):
                crashed_worker.append(victim)
                phase.log(f"{victim.name} CRASHED after Redis write, before commit (batch will be redelivered)")
            else:
                fault_errors.append("crash never triggered")
            w3 = spawn("w3")
            workers.append(w3)
            w3.ready.wait(30)
            phase.log("w3 joined (rebalance)")
            if redis_outage:
                subprocess.run(["docker", "pause", "redis"], check=True, capture_output=True)
                phase.log(f"redis PAUSED for {REDIS_PAUSE_S:.0f}s")
                time.sleep(REDIS_PAUSE_S)
                subprocess.run(["docker", "unpause", "redis"], check=True, capture_output=True)
                phase.log("redis unpaused")
            if connector_restart:
                status = restart_connector()
                phase.log(f"debezium connector RESTARTED (HTTP {status})")
            time.sleep(1.0)
            workers[1].stop()
            phase.log("w2 left the group (rebalance)")
        except Exception as e:  # never leave Redis paused
            fault_errors.append(f"fault injection error: {e!r}")
            subprocess.run(["docker", "unpause", "redis"], capture_output=True)

    t_start = time.time()
    ft = threading.Thread(target=faults, daemon=True) if phase.faults else None
    if ft:
        ft.start()
    with ThreadPoolExecutor(N_WRITERS) as pool:
        list(pool.map(writer, chunks, [random.Random(rng.random()) for _ in chunks]))
    phase.log(f"writes done in {time.time() - t_start:.1f}s: {op_counts}")
    if ft:
        ft.join(60)

    # ---- convergence + 100% consistency check
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        rows = {r[0]: {"name": r[1], "price": str(r[2]), "stock": r[3]} for r in conn.execute(
            "SELECT id, name, price, stock FROM products WHERE id = ANY(%s)", (ids,))}
    truth = {pk: rows.get(pk) for pk in ids}    # None = row deleted
    checker = workers[-1].inv

    def mismatches() -> list[tuple[int, object, object]]:
        bad = []
        for pk, want in truth.items():
            e = checker.get(pk)
            if want is None:
                ok = e is None or e.deleted
                got = None if ok else e.payload
            else:
                got = None if e is None or e.deleted else {k: e.payload.get(k) for k in want}
                ok = got == want
            if not ok:
                bad.append((pk, want, got))
        return bad

    live = [w for w in workers if not w.crashed.is_set() and not w._stop.is_set()]
    deadline = time.time() + CONVERGE_TIMEOUT_S
    bad = mismatches()
    while bad and time.time() < deadline:
        time.sleep(0.5)
        bad = mismatches()
    converge_s = time.time() - t_start
    time.sleep(1.5)     # let late replays land; they must not break consistency
    bad = mismatches()
    for w in workers:
        w.stop()
    for w in workers:
        w.thread.join(15)

    totals: dict[str, int] = {}
    for w in workers:
        for k, v in w.metrics.snapshot().items():
            if isinstance(v, int):
                totals[k] = totals.get(k, 0) + v
    deleted = sum(1 for v in truth.values() if v is None)
    print(f"  chaos: duplicated={sum(i.duplicated for i in interceptors)} "
          f"delayed_replays={sum(i.delayed for i in interceptors)}")
    print(f"  workers: {totals}")
    print(f"  converged in {converge_s:.1f}s from first write; {deleted} keys deleted at end; "
          f"{len(live)} workers alive at end")

    ok = True
    if bad:
        ok = False
        print(f"  FAIL: {len(bad)}/{len(truth)} keys inconsistent, e.g.:")
        for pk, want, got in bad[:5]:
            print(f"    id={pk} postgres={want} redis={got}")
    if totals.get("stale", 0) == 0:
        ok = False
        print("  FAIL: no stale events were rejected; the version gate was not exercised")
    if op_counts["delete"] == 0:
        ok = False
        print("  FAIL: workload produced no deletes")
    if phase.faults:
        for e in fault_errors:
            ok = False
            print(f"  FAIL: {e}")
        if not crashed_worker:
            ok = False
        if redis_outage and totals.get("redis_retries", 0) == 0:
            ok = False
            print("  FAIL: Redis outage did not trigger any worker retries")

    if phase.lags_ms:
        print(f"  invalidation lag over {len(phase.lags_ms)} applied writes: "
              f"p50={pctl(phase.lags_ms, 50):.1f}ms p99={pctl(phase.lags_ms, 99):.1f}ms "
              f"max={max(phase.lags_ms):.1f}ms mean={statistics.fmean(phase.lags_ms):.1f}ms"
              + ("  (includes injected outages)" if phase.faults else ""))
    else:
        ok = False
        print("  FAIL: no lag samples collected")
    print(f"  phase {phase.name}: {'PASS' if ok else 'FAIL'} - {len(truth) - len(bad)}/{len(truth)} keys consistent")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["steady", "faults", "all"], default="all")
    ap.add_argument("--no-redis-outage", action="store_true", help="skip docker pause (e.g. managed Redis)")
    ap.add_argument("--no-connector-restart", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level="ERROR", format="%(levelname)s %(name)s %(message)s")
    redis_outage = not args.no_redis_outage
    if redis_outage and not docker_available():
        print("note: docker/redis container not found; skipping Redis outage injection")
        redis_outage = False

    rng = random.Random(SEED)
    print(f"seed={SEED} keys={N_KEYS} ops={N_OPS} writers={N_WRITERS} "
          f"mix: {1 - P_DELETE - P_REINSERT:.0%} update / {P_DELETE:.0%} delete / {P_REINSERT:.0%} re-insert")
    results = []
    if args.phase in ("steady", "all"):
        results.append(run_phase(Phase("steady", faults=False), rng, False, False))
    if args.phase in ("faults", "all"):
        results.append(run_phase(Phase("faults", faults=True), rng, redis_outage, not args.no_connector_restart))
    ok = all(results)
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
