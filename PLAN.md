# PLAN: CDC-Driven Redis Cache Invalidation Engine

## 1. Objective
Build a production-grade Change Data Capture (CDC) cache invalidation pipeline that streams PostgreSQL WAL changes via Debezium and Apache Kafka into Redis, preventing out-of-order race conditions using LSN-checked Redis Lua scripts.

## 2. Architecture & Data Flow
`PostgreSQL (wal_level=logical)` -> `Debezium Postgres Connector` -> `Apache Kafka (KRaft)` -> `Python Async Invalidation Worker` -> `Redis (Atomic Lua Script)`

## 3. Tech Stack
- **Database:** PostgreSQL 16 (logical replication, publication/slot)
- **CDC & Streaming:** Debezium Connect (Apache 2.0), Apache Kafka in KRaft mode (single node, no ZooKeeper, Apache 2.0)
- **Cache:** Redis 8 (AGPLv3, Lua scripting)
- **Worker & Tests:** Python 3.11+, `confluent-kafka`, `redis`, `psycopg`, `pytest`
- **Orchestration:** Docker Compose
- **Licensing:** All components are OSI-approved open source

## 4. Directory Structure
```text
.
├── docker-compose.yml
├── postgres/
│   ├── init.sql              # Schema, replica identity, publication
│   └── postgresql.conf       # wal_level=logical, max_replication_slots
├── debezium/
│   └── register-connector.sh # Idempotent connector registration
├── worker/
│   ├── consumer.py           # Kafka consumer + DLQ routing
│   ├── invalidator.py        # Redis client + Lua script loader
│   └── update_if_newer.lua   # Atomic LSN comparison script
├── tests/
│   ├── test_lua_ordering.py  # Unit tests for out-of-order LSNs
│   └── chaos_test.py         # Concurrent write + staleness verification
└── README.md                 # Architecture RFC & 100x scale analysis
```

## 5. Phased Implementation

### Phase 1: Infrastructure & CDC Pipeline
- Create `docker-compose.yml` for PostgreSQL 16, Apache Kafka (KRaft), Debezium Connect, and Redis 8.
- Configure PostgreSQL with `wal_level=logical`, `products` table with `REPLICA IDENTITY FULL`, and a publication.
- Write `register-connector.sh` to register the Debezium PostgreSQL connector publishing to `dbserver1.public.products`.
- **Verification:** Insert a row in Postgres and confirm the JSON event (including `source.lsn`) appears in the Kafka topic.

### Phase 2: Atomic Redis Invalidation Worker
- Write `update_if_newer.lua`: takes `key`, `new_payload`, `new_lsn`, and `ttl`. Compares `new_lsn` against stored `lsn` in a Redis Hash. Only updates/invalidates if `new_lsn > stored_lsn`.
- Build `worker/consumer.py` to consume Debezium events (handle `c` create, `u` update, `d` delete, and `r` snapshot), extract `lsn`, and invoke the Lua script.
- Add Dead Letter Queue (DLQ) routing for malformed payloads and a background monitor that queries `pg_replication_slots` to alert if WAL lag exceeds a configurable byte threshold.
- **Verification:** `pytest tests/test_lua_ordering.py` passes, proving an older LSN never overwrites a newer LSN.

### Phase 3: Chaos & Concurrency Verification
- Write `tests/chaos_test.py` that:
  1. Fires 500 concurrent updates across 50 keys in PostgreSQL.
  2. Intentionally injects duplicate/delayed events into the worker.
  3. Asserts 100% consistency between PostgreSQL final state and Redis cached state.
- **Verification:** Script exits 0 and prints p50/p99 invalidation lag in milliseconds.

### Phase 4: Staff+ Documentation (README.md)
- Write a concise Architecture RFC covering:
  - Why LSN-based Lua gating beats naive `DEL` or `SET`.
  - Failure modes: replication slot bloat (disk full), Debezium snapshot locking, Redis eviction under memory pressure.
  - What changes at 100x scale (partitioning by primary key, multi-region replication).