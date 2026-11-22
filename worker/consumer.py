"""Debezium -> Redis invalidation worker.

Consumes Debezium change events for `public.products`, extracts `source.lsn`
and applies each change through the LSN-gated Lua script (see invalidator.py).

Delivery semantics: at-least-once. Offsets are committed only after the
Redis write (or DLQ produce) succeeds; redelivered events are harmless
because the Lua script rejects any (epoch, LSN) <= the stored version.

Run:  python -m worker.consumer
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

import redis
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

from worker.invalidator import Invalidator

log = logging.getLogger("cdc.worker")

# c=create u=update d=delete r=snapshot read t=truncate
# (truncate requires Debezium "skipped.operations": "none")
SUPPORTED_OPS = {"c", "u", "d", "r", "t"}


class MalformedEvent(ValueError):
    """Event cannot be processed and must go to the DLQ (never retried)."""


@dataclass(frozen=True)
class ChangeEvent:
    op: str                 # c | u | d | r | t
    pk: Any                 # None for truncate
    lsn: int
    row: dict[str, Any] | None   # after-image (None for deletes/truncates)
    source_ts_ms: int       # commit timestamp in Postgres


@dataclass
class Settings:
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    topic: str = os.getenv("CDC_TOPIC", "dbserver1.public.products")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "dbserver1.public.products.dlq")
    group_id: str = os.getenv("CONSUMER_GROUP", "cache-invalidator")
    pk_field: str = os.getenv("PK_FIELD", "id")
    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    # Longest wait to fill a batch: an upper bound on the latency the worker adds
    consume_timeout_s: float = float(os.getenv("CONSUME_TIMEOUT_S", "0.2"))
    # Monitoring only: needs pg_monitor (see postgres/init.sql), not superuser
    pg_dsn: str = os.getenv("PG_DSN", "postgresql://cdc_monitor:cdc_monitor@127.0.0.1:5432/inventory")
    wal_lag_threshold_bytes: int = int(os.getenv("WAL_LAG_THRESHOLD_BYTES", str(512 * 1024 * 1024)))
    wal_check_interval_s: float = float(os.getenv("WAL_CHECK_INTERVAL_S", "15"))
    # How long a slot may be inactive (e.g. during a connector restart) before alerting
    slot_inactive_grace_s: float = float(os.getenv("SLOT_INACTIVE_GRACE_S", "120"))
    redis_max_retries: int = int(os.getenv("REDIS_MAX_RETRIES", "10"))
    # Collapse each batch to the newest event per key before writing (safe; see coalesce())
    coalesce: bool = os.getenv("COALESCE", "true").lower() == "true"
    # Optional explicit start offsets {partition: offset}; overrides committed offsets
    start_offsets: dict[int, int] | None = None


def parse_event(value: bytes | None, key: bytes | None, pk_field: str = "id") -> ChangeEvent | None:
    """Parse a Debezium JSON envelope (schemas disabled).

    Returns None for Kafka tombstones (value=None), which carry no change.
    Raises MalformedEvent for anything that cannot be applied safely.
    """
    if value is None:
        return None
    try:
        env = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise MalformedEvent(f"invalid JSON: {e}") from e
    if not isinstance(env, dict):
        raise MalformedEvent("envelope is not a JSON object")
    # Tolerate schemas.enable=true envelopes
    if "payload" in env and "schema" in env:
        env = env["payload"]
        if not isinstance(env, dict):
            raise MalformedEvent("payload is not a JSON object")

    op = env.get("op")
    if op not in SUPPORTED_OPS:
        raise MalformedEvent(f"unsupported op: {op!r}")

    source = env.get("source")
    if not isinstance(source, dict):
        raise MalformedEvent("missing source block")
    lsn = source.get("lsn")
    if isinstance(lsn, bool) or not isinstance(lsn, int) or lsn < 0:
        raise MalformedEvent(f"missing/invalid source.lsn: {lsn!r}")
    ts = int(source.get("ts_ms") or env.get("ts_ms") or 0)

    if op == "t":
        return ChangeEvent(op="t", pk=None, lsn=lsn, row=None, source_ts_ms=ts)

    image = env.get("before") if op == "d" else env.get("after")
    if not isinstance(image, dict):
        raise MalformedEvent(f"missing {'before' if op == 'd' else 'after'} image for op={op}")

    pk = image.get(pk_field)
    if pk is None and key is not None:
        try:
            pk = json.loads(key).get(pk_field)
        except (ValueError, AttributeError):
            pk = None
    if pk is None:
        raise MalformedEvent(f"missing primary key field {pk_field!r}")

    return ChangeEvent(op=op, pk=pk, lsn=lsn, row=None if op == "d" else image, source_ts_ms=ts)


LAG_WINDOW = 10_000  # most recent samples kept for percentiles (bounded memory)


@dataclass
class Metrics:
    applied: int = 0
    stale: int = 0
    dlq: int = 0
    tombstones: int = 0
    truncates: int = 0
    coalesced: int = 0
    commit_failures: int = 0
    redis_retries: int = 0
    lag_ms: deque = field(default_factory=lambda: deque(maxlen=LAG_WINDOW))
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            lags = sorted(self.lag_ms)
            counters = {"applied": self.applied, "stale": self.stale, "dlq": self.dlq,
                        "tombstones": self.tombstones, "truncates": self.truncates,
                        "coalesced": self.coalesced, "commit_failures": self.commit_failures,
                        "redis_retries": self.redis_retries}
        def pct(q: float) -> float | None:
            return round(lags[min(len(lags) - 1, int(q * len(lags)))], 1) if lags else None
        return {**counters, "lag_p50_ms": pct(0.50), "lag_p99_ms": pct(0.99)}


# A record as seen by the processing pipeline: (key, value, kafka message or None)
Record = tuple[bytes | None, bytes | None, Message | None]
# Optional hook that may reorder/duplicate/drop records (used by chaos tests)
Interceptor = Callable[[list[Record]], Iterable[Record]]


class InvalidationWorker:
    def __init__(
        self,
        settings: Settings | None = None,
        invalidator: Invalidator | None = None,
        interceptor: Interceptor | None = None,
        on_applied: Callable[[ChangeEvent], None] | None = None,
    ) -> None:
        self.s = settings or Settings()
        self.inv = invalidator or Invalidator()
        self.inv.ensure_epoch()
        self.interceptor = interceptor
        self.on_applied = on_applied
        self.ready = threading.Event()  # set once partitions are assigned
        self.metrics = Metrics()
        self._stop = threading.Event()
        self.consumer = Consumer({
            "bootstrap.servers": self.s.bootstrap,
            "group.id": self.s.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            # cooperative rebalancing: unaffected partitions keep flowing
            "partition.assignment.strategy": "cooperative-sticky",
        })
        self.producer = Producer({"bootstrap.servers": self.s.bootstrap, "acks": "all", "enable.idempotence": True})

    # ---------------------------------------------------------------- processing
    def process_record(self, key: bytes | None, value: bytes | None, msg: Message | None = None) -> None:
        self.process_records([(key, value, msg)])

    def process_records(self, records: Iterable[Record]) -> None:
        """Parse, coalesce and apply a batch in one pipelined Redis round trip."""
        events: list[ChangeEvent] = []
        for key, value, msg in records:
            try:
                ev = parse_event(value, key, self.s.pk_field)
            except MalformedEvent as e:
                self._to_dlq(key, value, str(e), msg)
                continue
            if ev is None:
                with self.metrics.lock:
                    self.metrics.tombstones += 1
                continue
            events.append(ev)
        if not events:
            return

        batch, superseded = coalesce(events) if self.s.coalesce else (events, 0)
        results = self._apply_with_retry(batch)

        now_ms = time.time() * 1000
        with self.metrics.lock:
            self.metrics.coalesced += superseded
            for ev, applied in zip(batch, results):
                if applied:
                    self.metrics.applied += 1
                    if ev.op == "t":
                        self.metrics.truncates += 1
                    if ev.source_ts_ms and ev.op != "r":
                        self.metrics.lag_ms.append(now_ms - ev.source_ts_ms)
                else:
                    self.metrics.stale += 1
        if self.on_applied:
            for ev, applied in zip(batch, results):
                if applied:
                    self.on_applied(ev)

    def _apply_with_retry(self, batch: list[ChangeEvent]) -> list[bool]:
        changes = [(ev.op, ev.pk, ev.row, ev.lsn) for ev in batch]
        delay = 0.05
        for attempt in range(1, self.s.redis_max_retries + 1):
            try:
                # Re-sending the whole batch after a partial failure is safe:
                # every write is version-gated, so replays are no-ops.
                return self.inv.apply_batch(changes)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # Transient: retry, never DLQ. If retries are exhausted we raise,
                # the batch is not committed, and it is redelivered.
                log.warning("redis unavailable (attempt %d/%d): %s", attempt, self.s.redis_max_retries, e)
                with self.metrics.lock:
                    self.metrics.redis_retries += 1
                if attempt == self.s.redis_max_retries:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 2.0)
        raise AssertionError("unreachable")

    def _to_dlq(self, key: bytes | None, value: bytes | None, reason: str, msg: Message | None) -> None:
        headers = [("dlq.reason", reason.encode())]
        if msg is not None:
            headers += [
                ("dlq.source.topic", (msg.topic() or "").encode()),
                ("dlq.source.partition", str(msg.partition()).encode()),
                ("dlq.source.offset", str(msg.offset()).encode()),
            ]
        self.producer.produce(self.s.dlq_topic, key=key, value=value, headers=headers)
        with self.metrics.lock:
            self.metrics.dlq += 1
        log.error("routed to DLQ: %s", reason)

    # ---------------------------------------------------------------- main loop
    def run(self) -> None:
        def on_assign(consumer: Consumer, partitions: list) -> None:
            if self.s.start_offsets:
                for tp in partitions:
                    if tp.partition in self.s.start_offsets:
                        tp.offset = self.s.start_offsets[tp.partition]
            consumer.incremental_assign(partitions)
            self.ready.set()

        def on_revoke(consumer: Consumer, partitions: list) -> None:
            consumer.incremental_unassign(partitions)

        self.consumer.subscribe([self.s.topic], on_assign=on_assign, on_revoke=on_revoke)
        log.info("consuming %s (group=%s)", self.s.topic, self.s.group_id)
        try:
            while not self._stop.is_set():
                msgs = self.consumer.consume(num_messages=self.s.batch_size, timeout=self.s.consume_timeout_s)
                if not msgs and not self.interceptor:
                    continue  # (interceptors get idle ticks so they can release held records)
                records: list[Record] = []
                offsets: dict[tuple[str, int], int] = {}
                for m in msgs:
                    if m.error():
                        if m.error().code() != KafkaError._PARTITION_EOF:
                            log.error("kafka error: %s", m.error())
                        continue
                    records.append((m.key(), m.value(), m))
                    tp = (m.topic(), m.partition())
                    offsets[tp] = max(offsets.get(tp, -1), m.offset())
                stream: Iterable[Record] = self.interceptor(records) if self.interceptor else records
                self.process_records(stream)
                # DLQ writes must be durable before we commit past them
                if self.producer.flush(10) > 0:
                    raise RuntimeError("DLQ flush timed out; not committing")
                if offsets:
                    self._commit(offsets)
        finally:
            self.consumer.close()
            self.producer.flush(5)

    def _commit(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit exactly what this batch processed. A commit that fails because a
        rebalance moved the partition is harmless: the new owner re-reads from the
        last committed offset and the version gate turns the replay into no-ops."""
        # Only commit partitions we still own: after a cooperative rebalance a late
        # commit for a moved partition could rewind the new owner's committed offset.
        owned = {(tp.topic, tp.partition) for tp in self.consumer.assignment()}
        tps = [TopicPartition(t, p, o + 1) for (t, p), o in offsets.items() if (t, p) in owned]
        if not tps:
            return
        try:
            self.consumer.commit(offsets=tps, asynchronous=False)
        except KafkaException as e:
            log.warning("offset commit failed (will be redelivered): %s", e)
            with self.metrics.lock:
                self.metrics.commit_failures += 1

    def stop(self) -> None:
        self._stop.set()


def coalesce(events: list[ChangeEvent]) -> tuple[list[ChangeEvent], int]:
    """Keep only the highest-LSN event per key (and per truncate stream).

    Safe because version-gated writes commute: the visible final state of a key
    depends only on the maximum version applied, not on the order of arrival.
    Returns (events to apply, number superseded)."""
    best: dict[Any, ChangeEvent] = {}
    for ev in events:
        k = ("t",) if ev.op == "t" else ("k", ev.pk)
        cur = best.get(k)
        if cur is None or ev.lsn > cur.lsn:
            best[k] = ev
    out = list(best.values())
    return out, len(events) - len(out)


class WalLagMonitor(threading.Thread):
    """Alerts on replication-slot conditions that endanger the primary or the cache.

    - retained WAL above threshold: a lagging/inactive slot pins WAL on the
      primary and can fill its disk (the #1 operational risk of logical CDC)
    - wal_status 'unreserved'/'lost': the slot exceeded max_slot_wal_keep_size;
      once lost, CDC has stopped and the cache is silently stale until a
      re-snapshot (new slot + epoch bump)
    - slot inactive beyond a grace period: connector down
    - slot missing entirely: dropped or never created
    """

    QUERY = """
        SELECT slot_name, active, wal_status,
               pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag_bytes
        FROM pg_replication_slots
        WHERE slot_type = 'logical'
    """

    def __init__(self, dsn: str, threshold_bytes: int, interval_s: float,
                 slot_name: str = os.getenv("SLOT_NAME", "debezium_slot"),
                 inactive_grace_s: float = 120.0,
                 on_alert: Callable[[str, str], None] | None = None) -> None:
        super().__init__(daemon=True, name="wal-lag-monitor")
        self.dsn, self.threshold, self.interval = dsn, threshold_bytes, interval_s
        self.slot_name, self.inactive_grace_s = slot_name, inactive_grace_s
        self.on_alert = on_alert or (lambda slot, msg: log.critical("REPLICATION SLOT ALERT slot=%s %s", slot, msg))
        self._inactive_since: dict[str, float] = {}
        self._stop = threading.Event()

    def evaluate(self, rows: list, now: float | None = None) -> list[tuple[str, str]]:
        """Pure decision logic over (slot, active, wal_status, lag_bytes) rows.

        Returns (and emits) the alerts raised."""
        now = time.monotonic() if now is None else now
        alerts: list[tuple[str, str]] = []
        if self.slot_name and self.slot_name not in {r[0] for r in rows}:
            alerts.append((self.slot_name, "slot missing: CDC is not running"))
        for slot, active, wal_status, lag in rows:
            if wal_status in ("lost", "unreserved"):
                alerts.append((slot, f"wal_status={wal_status}: slot exceeded max_slot_wal_keep_size; "
                                     "re-snapshot and bump CDC_EPOCH"))
            if lag is not None and int(lag) > self.threshold:
                alerts.append((slot, f"retained WAL {int(lag)} bytes > threshold {self.threshold}"))
            if active:
                self._inactive_since.pop(slot, None)
            else:
                since = self._inactive_since.setdefault(slot, now)
                if now - since >= self.inactive_grace_s:
                    alerts.append((slot, f"inactive for {now - since:.0f}s: connector not consuming"))
        for slot, msg in alerts:
            self.on_alert(slot, msg)
        return alerts

    def check_once(self) -> list[tuple[str, str]]:
        import psycopg
        with psycopg.connect(self.dsn, connect_timeout=5) as conn:
            rows = conn.execute(self.QUERY).fetchall()
        return self.evaluate(rows)

    def run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.check_once()
            except Exception as e:  # monitoring must never kill the worker
                log.warning("replication slot check failed: %s", e)

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = Settings()
    worker = InvalidationWorker(s)
    monitor = WalLagMonitor(s.pg_dsn, s.wal_lag_threshold_bytes, s.wal_check_interval_s,
                            inactive_grace_s=s.slot_inactive_grace_s)
    # An empty PG_DSN means this worker has no database route (secondary side).
    monitor_enabled = bool(s.pg_dsn)
    if monitor_enabled:
        monitor.start()
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    worker.run()
    if monitor_enabled:
        monitor.stop()
    log.info("stopped: %s", worker.metrics.snapshot())


if __name__ == "__main__":
    main()
