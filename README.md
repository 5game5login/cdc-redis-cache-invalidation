# CDC-Driven Redis Cache Invalidation Engine

PostgreSQL WAL changes stream through Debezium and Kafka into Redis. Every
cache write goes through an atomic Lua script that applies a change **only
if its version `(epoch, LSN)` is newer** than the one already cached. This
prevents stale overwrites caused by reordering, redelivery, replay, reader
fills, truncates and database restores.

```
PostgreSQL 16 ──WAL (pgoutput)──▶ Debezium Connect ──▶ Kafka (KRaft)
 wal_level=logical                 slot: debezium_slot    dbserver1.public.products
 REPLICA IDENTITY DEFAULT        user: debezium         4 partitions, keyed by PK
                                 (REPLICATION, no superuser)   │
                                                               ▼
                       Redis 8  ◀── pipelined EVALSHA ──── Python workers (consumer group)
                       (AUTH)       update_if_newer.lua    (DLQ + slot monitor as cdc_monitor)
     product:{id} = {epoch, lsn, payload, deleted, fill}
     cdc:truncate:product = {epoch, lsn}   cdc:epoch = N
                          ▲
                          └── app readers: Invalidator.read_through() (fill-if-absent)
```

## Quick start

```bash
docker compose up -d --wait
bash debezium/register-connector.sh            # idempotent (PUT .../config)
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt   # bin/ on Linux/macOS

.venv/Scripts/python -m pytest tests/test_lua_ordering.py   # 62 unit tests
.venv/Scripts/python tests/chaos_test.py                    # steady + fault-injection phases
CDC_EPOCH=0 .venv/Scripts/python -m worker.consumer         # run a worker (start several to scale)
```

All local defaults (passwords included) match `docker-compose.yml` and
`postgres/init.sql`, and every port is bound to `127.0.0.1`. Clients use
`127.0.0.1` rather than `localhost`: on Windows, `localhost` tries IPv6
`::1` first and adds about 2 s to every connection.

| Path | Purpose |
|---|---|
| `postgres/init.sql`, `postgresql.conf` | Schema, `REPLICA IDENTITY DEFAULT`, publication (incl. truncate), least-privilege `debezium` / `cdc_monitor` roles, `wal_level=logical`, `max_slot_wal_keep_size` |
| `debezium/register-connector.sh` | Idempotent connector registration as the `debezium` role, truncate events enabled |
| `worker/update_if_newer.lua` | Atomic `(epoch, LSN)` compare-and-set: `set`, `del` (tombstone), `truncate` (watermark), `fill` (read path) |
| `worker/invalidator.py` | Redis client, pipelined `apply_batch()`, `read_through()`, epoch guard, script loader (`EVALSHA`, auto-reload on `NOSCRIPT`) |
| `worker/consumer.py` | Kafka consumer group (`c`/`u`/`d`/`r`/`t`): per-batch coalescing, per-partition commits, DLQ, Redis retry, bounded metrics, replication-slot monitor |
| `tests/test_lua_ordering.py` | 62 tests: ordering, duplicates, tombstones, 64-bit LSNs, permutations, epochs, fills, truncate, batching equivalence, parsing, slot monitor |
| `tests/chaos_test.py` | Two phases on live infra; asserts PG equals Redis for every key (below) |

| Env var | Default | Meaning |
|---|---|---|
| `CDC_EPOCH` | `0` | Version generation; bump on restore or re-snapshot (see runbook) |
| `CACHE_TTL_SECONDS` | `3600` | TTL for CDC-written entries and tombstones |
| `FILL_TTL_SECONDS` | `60` | TTL for reader fills (bounds the residual race) |
| `COALESCE` | `true` | Collapse each batch to the newest event per key before writing |
| `CONSUME_TIMEOUT_S` | `0.2` | Longest wait to fill a batch (0.02 on AWS) |
| `REDIS_PASSWORD` / `REDIS_TLS` | `cdc-local-dev` / `false` | Redis auth / TLS |
| `REDIS_MAX_RETRIES`, `REDIS_SOCKET_TIMEOUT_S` | `10`, `5` | Worker-level retry budget (about 70 s worst case, below `max.poll.interval.ms`) |
| `PG_DSN` | `cdc_monitor@127.0.0.1` | Slot monitor connection (`pg_monitor` role only) |
| `WAL_LAG_THRESHOLD_BYTES` | 512 MiB | Retained-WAL alert threshold |
| `SLOT_NAME` / `SLOT_INACTIVE_GRACE_S` | `debezium_slot` / `120` | Slot expected to exist, and how long it may be inactive |

### Verification

`tests/chaos_test.py` runs two phases, each on 50 fresh keys with 500
concurrent writes (80% update, 10% delete, 10% re-insert of a deleted
ID). Two or three workers in one consumer group share 4 partitions. Each
worker's input is shuffled, about 30% of events are duplicated, and about
25% are replayed late. Each phase asserts that every key matches
PostgreSQL: present and equal, or deleted.

| Phase | Extra faults injected while writes are in flight |
|---|---|
| `steady` | none. Reports invalidation lag. |
| `faults` | a worker **crashes after writing Redis but before committing offsets**; a worker **joins** and another **leaves** (two rebalances); **Redis is frozen** for 7 s with `docker pause`; the **Debezium connector restarts** (`--no-redis-outage` and `--no-connector-restart` skip the last two) |

**Measured locally** (Docker Desktop on Windows, single node, last 3 full runs):
- 100% of keys consistent in every phase of every run.
- Workers rejected 48–93 stale or duplicate events per phase, and
  coalescing absorbed another 350–470.
- The Redis freeze triggered worker retries every time, with no data loss.

Invalidation lag (host commit to Redis apply):
- **steady:** p50 1.1–1.2 s, p99 2.7–3.0 s.
- **faults:** p99 7.9–8.8 s, dominated by the 7 s freeze.

Earlier runs on a single partition with one worker measured p50 0.56–1.03 s.
I haven't profiled the difference. The Lua and pipeline time is small, so
the likely tuning knobs are in Debezium (`poll.interval.ms`,
`max.batch.size`) and the consumer (`fetch.wait.max.ms`).
TRUNCATE was verified separately against the live stack.

### Measured on AWS (2026-11-21, us-east-2)

Deployed from `infra/aws-1region`: Aurora PG 16.13 Serverless v2 (writer + reader), two
ElastiCache Valkey 8.2 caches, and two VPCs with **the same CIDR** joined only by
PrivateLink (mTLS Kafka + MirrorMaker 2). Raw results are in `results/aws/`.

500 committed writes per run (about 80% update, 10% delete, 10% re-insert) on 50 keys;
3 runs per phase. Every run ended with 50/50 keys equal to Aurora in **both** caches.

| Phase | Cache A p50 / p99 (ms) | Cache B via PrivateLink p50 / p99 (ms) |
|---|---|---|
| epoch 0 | 439-576 / 1018-1123 | 461-616 / 1093-1142 |
| epoch 1, after failover | 525-648 / 879-1062 | 540-661 / 895-1071 |

- **Failover:** the writer role moved in 29 s. The logical slot was still present and
  active, and Debezium resumed by itself; first confirmed 111 s after the start (the
  check polls through SSM, so this is an upper bound).
- **Epoch runbook:** both caches advanced 0 -> 1, and an epoch-0 worker refused to start on both.
- **Isolation:** node B reaches only `kafka-a:9094`. Aurora, cache A and Kafka 9092 are blocked.

**Continuous writes through a failover, no epoch bump** (`scripts/run_failover_soak.py`,
`results/aws/soak-failover-1.json`): 9,604 new keys and 13,400 acknowledged commits over
240 s, with the failover requested 45 s in.

| | Result |
|---|---|
| Acknowledged inserts missing from Aurora | 0 |
| Keys differing from Aurora, cache A / cache B | 0 / 0 of 9,604 |
| Longest pause in writes (clients retry and reconnect) | 1.7 s |
| Longest staleness, cache A / cache B | 17.5 s / 17.6 s |
| Epoch, slot, topic | unchanged; the slot was still on the new writer |

Each key is inserted once, with at most one later update or delete, so a lost final event
cannot be hidden by a later write. A hidden re-snapshot is ruled out: Debezium logged
`SnapshotResult [status=SKIPPED]` and resumed from its stored LSN. About 10 s of the 17.5 s
is Debezium's `retriable.restart.connector.wait.ms` (default 10000), tuned below.
Restores, rebuilds and lost slots still need the epoch runbook.

**Latency tuning (2026-11-22).** `stage_probe.py` splits the lag using the timestamps in
each event. Debezium's `poll.interval.ms` (default 500) is waited out *twice*, once as an
idle sleep before reading WAL and once in the queue hand-off; the worker then waited up to
200 ms to fill a batch. MirrorMaker + PrivateLink add only a few ms. One change at a time,
30 s at 40 inserts/s, p50 in ms:

| Setting | commit -> Debezium | Debezium -> Kafka | Kafka -> A / B | total A / B |
|---|---|---|---|---|
| defaults (500 ms poll, 200 ms batch wait) | 248 | 276 | 118 / 120 | 628 / 638 |
| `poll.interval.ms` = 50 | 24 | 25 | 119 / 132 | 172 / 183 |
| + worker batch wait 20 ms | 24 | 26 | 32 / 33 | **80 / 83** |

Benchmark after tuning (3 runs, 50/50 keys on both sides each time): A p50 62-65, p99
110-115 ms; B p50 68-76, p99 121-123 ms. Values live in `node/tuning.env`; the local
stack keeps the defaults. Not measured: whether smaller batches hurt at high load, where
coalescing matters more.

**Failover after tuning.** Same test each time (240 s, 9,604 keys), settings added
cumulatively (the second run changed three at once). No run lost or changed a key on either side, and every run skipped the snapshot.

| Run | Debezium noticed after the writers | Longest staleness A / B | Write pause |
|---|---|---|---|
| defaults | 6.2 s | 17.5 / 17.6 s | 1.7 s |
| + 50 ms poll, 20 ms batch wait, 1 s restart wait | 8.5 s | 10.8 / 10.8 s | 2.3 s |
| + `status.update.interval.ms` = 1000 | **0.4 s** | **2.6 / 2.6 s** | 1.6 s |

| same settings, 3 repeats | 0.1 / 0.1 / 0.5 s | 2.2 / 2.3 / 3.1 s (A and B equal) | 1.4-1.9 s |

Debezium notices a dead connection only when it next *sends* to it, and by default it sends
a status update every 10 s; detection with that default varies with where in the 10 s
window the failover lands.

**Writer crash** (`--crash`, Aurora fault injection). The first run **failed**: Debezium's
restart met "connection refused" while Aurora was still recovering, which kills the task for
good. 6,263 of 9,604 keys were still stale 14 minutes later (`soak-crash-1.json`). The 1 s
restart wait above made this more likely. `connector_watchdog.py` now restarts a failed task,
only when the database is read-write and the slot is intact; it recovered all 9,604 keys of
that run. Two more crashes with it passed (7,204 keys each, 0 lost or different, worst
staleness 1.9 and 2.2 s, writes paused about 12 s while Aurora recovered).

**Load:** 200 inserts/s for 2 minutes, 24,020 of 24,020 keys correct on both sides, p50
84 / 88 ms, p99 153 / 162 ms. **Full Aurora stop and start:** slot survived, recovered unaided.

Full design, failure model and limits: [docs/RFC-001](docs/RFC-001-cdc-cache-invalidation.md).

Read these with care:
- Lag is commit acknowledged -> first seen by a 5 ms poll. Versions merged by coalescing
  are never seen, so each side samples a different 171-236 of about 450 row writes. That is why
  B's p50 can look lower than A's. Small A/B differences are not meaningful.
- The 500-write benchmarks last about 3 s each, and the heaviest run is 200 inserts/s for
  2 minutes on small nodes. None of this is a capacity test.
- Faults were requested failovers and injected crashes: no AZ loss, partition or real restore.
- Both nodes are burstable instances with `unlimited` credits. With `standard` credits node B
  was throttled to 20% CPU and failed its health check.

---

## RFC: why LSN-gated Lua instead of `DEL` or `SET`

**Problem.** Kafka gives ordering only within a partition, and only for one
delivery attempt. Real systems still deliver changes out of order:
consumer rebalances and at-least-once redelivery replay old offsets,
connector restarts re-emit events from the last flushed offset, snapshots
(`op=r`) interleave with streaming, and retries from parallel workers race.

| Strategy | Failure |
|---|---|
| **Naive `SET` of the event payload** | A late or redelivered older event overwrites newer data, and the cache stays wrong until the TTL expires. |
| **Naive `DEL` (invalidate and let readers repopulate)** | It's a read/write race. A reader misses, reads PG (old), the writer commits and the CDC `DEL` lands, then the reader `SET`s the old value it read. The cache is now stale with no future event to fix it. It also causes thundering-herd misses on hot keys. |
| **App-level read-compare-write** | Not atomic: two workers can both read `lsn=100` and both write. |
| **Lua `update_if_newer`** | Compare and write are one atomic step inside Redis (scripts run single-threaded). The LSN is a total order over the WAL, so the cache converges to the latest committed state no matter how events arrive. Duplicates (`lsn == stored`) are no-ops, which makes at-least-once delivery effectively exactly-once at the cache. |

Design details:

- **Deletes write tombstones** (`deleted=1, lsn=N`) instead of `DEL`, so a
  delayed older `UPDATE` cannot bring a deleted row back. Tombstones carry
  the same TTL.
- **LSNs are compared as decimal strings.** A PG LSN is 64-bit, and Lua
  numbers are doubles (exact only up to 2^53). The tests cover `2^64-1`.
- **Snapshot rows share one LSN** (the snapshot's consistent point). That is
  safe because gating is per key and every later streamed change has a
  strictly greater LSN.
- **Offsets are committed only after Redis and DLQ writes succeed.** Redis
  outages are retried with backoff and never sent to the DLQ. If retries run
  out, the batch is redelivered. Only *structurally* malformed events (bad
  JSON, unknown `op`, missing `lsn` or PK) go to `<topic>.dlq`, with
  reason and source topic/partition/offset headers.
- **Versions are `(epoch, lsn)`.** Change LSNs are monotonic per row,
  because row locks serialise writers, so within one database lifetime the
  LSN alone orders a key's versions. The epoch handles the cases where WAL
  positions go *backwards* (see the runbook below).

### Read path: `Invalidator.read_through(pk, loader)`

A hit returns the payload, and a tombstone returns `None` without touching
PG. On a miss the app loads the row and calls **`fill`**. A fill writes only
if no valid entry exists, and it is stamped `(epoch, 0)` with `fill=1`, so
**any** real CDC event for the key supersedes it.

A reader must never tag its fill with a real LSN such as
`pg_current_wal_lsn()`. An earlier version of this README recommended
exactly that, and it is unsafe:
1. A writer's UPDATE is already in WAL at LSN Y but not yet committed.
2. A reader's snapshot still sees the old row, and `pg_current_wal_lsn()`
   returns X > Y.
3. The reader caches the old row at X.
4. The CDC event at Y is rejected as older, so the cache stays wrong until
   the TTL expires.

**Residual race (bounded, not eliminated):**
1. A reader loads v1.
2. CDC writes v2.
3. The key is evicted or expires.
4. The reader's fill lands with v1.

Nothing corrects it until the next change or `FILL_TTL_SECONDS` (default
60 s). That's why fills get a short TTL. The loader must return the same
JSON shape Debezium emits (NUMERIC as string, ISO-8601 UTC timestamps).

### TRUNCATE

Debezium emits `op=t` when `skipped.operations=none` is set (it is in
`register-connector.sh`). The worker raises a per-table watermark
`cdc:truncate:product = (epoch, lsn)`. Entries at or below the watermark
become invisible to `get()` and can't be overwritten by delayed
pre-truncate events. The leftover keys age out via TTL instead of an
O(N) scan. A fill made *after* a truncate is stamped at the watermark, so
it stays valid, and the next CDC event still supersedes it.

### Epoch runbook: when WAL positions can go backwards

Bump the epoch after any of these: restore from snapshot or PITR, rebuild
from a dump, a major-version upgrade that recreates the slot, failover to
a node without the slot, or a slot that ended up `lost`.
1. Stop the worker.
2. Register a **new connector with a new `topic.prefix` and slot name**.
   It takes a fresh initial snapshot.
3. Start the worker with `CDC_EPOCH=<previous+1>` and `CDC_TOPIC=<new topic>`.

The worker stamps events with *its configured* epoch, so a new-epoch worker
must never consume the old topic. Old-epoch events would be stamped as new
and beat newer data. On startup the worker records the epoch in
`cdc:epoch` and **refuses to run with a lower epoch** than Redis has seen.

The snapshot rewrites every row that still exists. Rows that exist only in
the old database have no delete event and stay cached until their TTL. If
that window is unacceptable, flush the table's keys at step 1.

## Failure modes

**1. Replication slot bloat (disk full on the primary). This is the
highest-severity risk.** A logical slot pins WAL until Debezium confirms it.
If Connect is down, the connector fails, or the connector is deleted while
the slot stays, WAL grows without bound and fills the primary's disk. That
outage hits the *database*, not just the cache.
- *Mitigations in this repo:* `max_slot_wal_keep_size = 2GB` makes PG
  invalidate the slot instead of filling the disk. That trades the disk
  problem for a forced re-snapshot. `WalLagMonitor` alerts on:
  - retained WAL above `WAL_LAG_THRESHOLD_BYTES`
  - `wal_status` of `unreserved` or `lost` (CDC has stopped and the cache is
    silently stale)
  - the slot staying inactive beyond `SLOT_INACTIVE_GRACE_S`
  - the slot missing entirely
- *Caveat:* `heartbeat.interval.ms` alone may not advance the slot when the
  captured tables are quiet but other tables in the database are busy. That
  usually also needs `heartbeat.action.query` writing to a table in the
  publication. It isn't configured here.
- *Runbook:* drop orphaned slots deliberately. A `lost` slot means events
  were missed, so follow the epoch runbook.

**2. Debezium snapshot locking and load.** The initial snapshot runs a
long repeatable-read transaction over every captured table. That holds back
`xmin` (vacuum can't clean, so tables bloat) and can briefly take locks
that block DDL. On large tables it can run for hours.
- *Mitigations:* use incremental snapshots (`signal` table, chunked by PK,
  no long transaction, interleaved with streaming, and safe here because
  of LSN gating). Snapshot from a replica where possible, and schedule
  snapshots off-peak. Use `snapshot.mode=no_data` when the cache can warm
  lazily.

**3. Redis eviction or TTL expiry.** Removing a key also removes its stored
version, so the next event for that key is accepted unconditionally.
Within one partition a replay always ends with the newest event, so this
is normally temporary. It becomes lasting only via the read-path residual
race above, which `FILL_TTL_SECONDS` bounds.
- *Mitigations:* `maxmemory-policy volatile-lru` with a TTL on every entry,
  so only cache entries are eviction candidates. The truncate watermark and
  `cdc:epoch` have no TTL. Alert on `evicted_keys`. Where even the bounded
  window is unacceptable, keep version watermarks in a separate `noeviction`
  instance or keyspace.

Other failure modes to know about:
- **Kafka retention shorter than worker downtime:** events are lost, so
  follow the epoch runbook.
- **Schema changes:** Debezium emits the new shape. `parse_event` only
  needs the PK and `source.lsn`, so extra or missing columns pass through.
- **Primary failover (Aurora):** in our test the slot survived an in-cluster failover
  (see "Measured on AWS").
- **Primary failover (community PostgreSQL):** logical slots aren't carried over before PG 17
  failover slots, so this is an epoch-runbook event.

### Known limitations

- **Multi-row transactions are not atomic in the cache. This is a
  deliberate trade-off.** Every key converges to its committed state, but
  for a short window (normally under the invalidation lag) a reader can see
  some rows of a transaction updated and others not. Making transactions
  atomic would need three things, and each one works against the design:
  1. Debezium transaction metadata (`provide.transaction.metadata`), with
     the worker buffering every transaction until its `END` marker arrives.
     That adds latency and memory, and it needs offset management that
     never commits past an incomplete transaction.
  2. All of a transaction's events consumed by **one** worker. But events
     are partitioned by primary key, so a transaction is spread across
     partitions and workers. Partitioning by transaction instead would
     give up per-key locality.
  3. One atomic multi-key Redis write. In Redis Cluster that requires every
     key in the same slot, which defeats sharding the cache by key.
  
  If you need transactionally consistent reads, either read those entities
  from PostgreSQL, or cache the aggregate as **one key** (e.g. an order and
  its lines under `order:{id}`), so the version gate covers the whole
  aggregate.
- **The script touches two keys** (the entry and the table watermark), so
  it needs a non-cluster Redis (primary + replicas), or both keys in one
  cluster slot. See scale item 1.
- **Local infrastructure is still single-node:** Kafka and Connect use
  replication factor 1, and nothing uses TLS. Those belong to the deployment
  (e.g. MSK with replication factor 3 and TLS/IAM, ElastiCache with
  in-transit encryption). Local hardening that is in place: no superuser
  for CDC or monitoring, Redis AUTH, all ports bound to `127.0.0.1`, and
  `REPLICA IDENTITY DEFAULT` rather than FULL (PLAN.md originally specified
  FULL; deletes only need the PK).
- **Crash and failure scope in the chaos test:** workers crash cleanly,
  closing the consumer. A hard kill takes the same path after
  `session.timeout.ms`. Redis is frozen, not restarted: without
  persistence, a restart empties the cache and readers fall back to
  `read_through`.

## What changes at 100x scale

Roughly 100x means about 200k changes/s across many tables, a cache tier
in the hundreds of GB, and readers in several regions.

1. **Partition by primary key, end to end.** Debezium already keys
   messages by PK, so give the topic N partitions (for example 64–256).
   Per-key order then holds within a partition and the worker scales
   horizontally with one consumer per partition. LSN gating stays as the
   safety net for rebalances and replays. For Redis Cluster, the entry and
   its truncate watermark must share a slot. Bucket keys with a hash tag,
   e.g. `product:{b17}:42` where `b = hash(id) % B`, and keep one watermark
   per bucket (`cdc:truncate:product:{b17}`). A TRUNCATE then writes B
   watermarks. That's cheap, and per-key ordering is unaffected.
2. **Batching (implemented).** Each consumed batch is coalesced to the
   max-version event per key and written as one pipeline. Offsets are
   committed per partition after the pipeline succeeds. This is safe
   because version-gated writes commute: the final state depends only on
   the maximum version, not on arrival order. A unit test checks that the
   batched path matches applying events one by one. At scale, tune
   `BATCH_SIZE` and run one worker per partition.
3. **The single slot becomes the bottleneck.** One Debezium task decodes
   all WAL. Split it into per-table or per-schema connectors, each with its
   own slot and publication. Move capture to a replica on PG 16+ (logical
   decoding on standbys) so decoding CPU is off the primary. Each slot is
   one more place WAL can pile up, so slot-lag alerting becomes a
   tier-1 page.
4. **Multi-region.** Keep one writer region as the source of truth. Use
   MirrorMaker 2 or Cluster Linking to replicate the CDC topic to each
   region, and run a regional worker against a regional Redis. Don't
   replicate Redis itself. LSNs from a single primary are globally
   comparable, so regional caches converge to the same state independently.
   Failover and promotion are handled by the existing `(epoch, lsn)`
   versioning. Automate the epoch bump in the promotion runbook. With
   multi-primary writes, LSNs are no longer comparable, and you'd need
   per-row version columns or HLCs instead.
5. **Operations.** Schema Registry with Avro/Protobuf instead of JSON (about
   5–10x smaller payloads). Make the DLQ replayable with tooling. Track
   end-to-end lag as `now - source.ts_ms` per partition with SLO alerts.
   Gate cache reads on lag (bypass the cache when CDC lag is above SLO) for
   reads that need strong consistency.
