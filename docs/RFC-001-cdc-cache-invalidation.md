# RFC-001: Version-gated cache invalidation from PostgreSQL CDC

| | |
|---|---|
| Status | Implemented and tested as a proof of concept. Not run in production. |
| Date | 2026-11-22 |
| Code | `worker/` (engine), `infra/aws-1region/` (AWS deployment and tests) |
| Evidence | `results/aws/`, summarized in the README under "Measured on AWS" |

## 1. Summary

Keep Redis/Valkey caches consistent with PostgreSQL by streaming WAL changes through
Debezium and Kafka into a worker that writes the cache **only if the change is newer** than
what is cached. "Newer" is decided atomically inside Redis on a version `(epoch, LSN)`, so
reordering, duplicates, replays and parallel workers cannot leave an old value behind.

A second, isolated network gets its own copy of the stream through one mTLS port and runs
its own worker and cache.

On AWS, with Aurora failing over or crashing under continuous writes, no acknowledged write
was lost and every key converged in both caches, with one exception that the testing found
and this RFC fixes: after a writer crash, Debezium could stop permanently (section 7).

## 2. Problem

A cache beside a database goes stale in ways that a TTL only hides:

- **`SET` the event payload:** a late or redelivered older event overwrites newer data.
- **`DEL` and let readers refill:** a reader can load the old row, lose the race to the
  `DEL`, then write the old row back. No later event corrects it.
- **Read, compare, write in the app:** not atomic; two workers can both win.

Out-of-order delivery is normal: consumer rebalances, at-least-once redelivery, connector
restarts and snapshots interleaved with streaming.

## 3. Goals and non-goals

**Goals**
1. After writes stop, every cached key equals the database, whatever the delivery order.
2. No acknowledged write is lost to the cache across worker, cache, connector or database failure.
3. A second site that can read changes and nothing else.
4. Failures that could silently skip events must alert, not self-heal.

**Non-goals**
- Multi-row transactional consistency in the cache (each key converges independently).
- Multi-primary databases: LSNs from two primaries are not comparable.
- A throughput record. The load tested is modest (section 8).

## 4. Design

```
PostgreSQL / Aurora --WAL--> Debezium --> Kafka A --> worker A --> cache A
                                             |
                              mTLS :9094, read-only ACL, PrivateLink
                                             v
                         MirrorMaker 2 --> Kafka B --> worker B --> cache B
```

### 4.1 Version gate
`worker/update_if_newer.lua` runs in Redis, so compare and write are one atomic step.

- A version is `(epoch, lsn)`, compared lexicographically. Both are **decimal strings**: an
  LSN is 64-bit and Lua numbers are exact only to 2^53.
- A change applies only if its version is greater than the stored one and than the table's
  truncate watermark.
- A delete writes a **tombstone** with its version, so a delayed older update cannot
  resurrect the row.
- A reader's fill is stamped below every real change, so any CDC event supersedes it. A
  reader must never claim a real LSN: an uncommitted change may already be in WAL.

*Why it converges.* Row locks serialize writers of a row, so its changes have increasing
LSNs. The gate keeps the maximum version seen, and "maximum" does not depend on order or
repetition. That makes writes commutative and idempotent, which is also why a batch can be
coalesced to the newest event per key and retried whole.

### 4.2 Epoch
WAL positions can go backwards after a restore, a rebuild or a recreated slot. The epoch is
an operator-controlled generation that outranks the LSN. A worker refuses to start with a
lower epoch than the cache has seen.

### 4.3 Delivery
Offsets are committed only after the Redis write or dead-letter write succeeds. Redis
outages are retried, never dead-lettered. Only structurally invalid events go to the DLQ.

### 4.4 Second site
The two AWS VPCs use **the same CIDR on purpose** and have no peering. The only path is
PrivateLink to Kafka's port 9094. Access is layered: allowed principals, endpoint security
group, mTLS, and an ACL allowing the client certificate to read the CDC topics only.
MirrorMaker keeps topic names unchanged, so both workers run the same configuration.

LSNs from one primary are comparable everywhere, so the second cache converges on its own.
Redis itself is not replicated between sites.

### 4.5 Connector watchdog
`connector_watchdog.py` restarts a `FAILED` Debezium task **only if** the database is
reachable and read-write, and the replication slot exists and is not `lost`/`unreserved`.

A missing slot is an alert, never a restart: Debezium can create a new slot at the current
position and silently skip everything in between. That case belongs to the epoch runbook.

## 5. Alternatives considered

| Option | Why not |
|---|---|
| TTL only | Bounded but routine staleness; a long TTL is wrong and a short one defeats the cache |
| `DEL` on change | Refill race above, and miss storms on hot keys |
| Application dual writes | No atomicity across database and cache; misses out-of-band writes |
| Transactional outbox | Sound, but every writer must change; CDC captures all writers |
| Replicate Redis to the second site | Couples the sites and ships errors along with data |
| VPC peering / Transit Gateway | Routes whole networks; impossible with overlapping CIDRs |
| Restart the connector unconditionally | Can hide a missing slot and skip events |

## 6. Failure model

| Failure | Behaviour | Tested |
|---|---|---|
| Reordered, duplicated, replayed events | Rejected by the gate | Unit + local chaos test |
| Worker dies after Redis write, before commit | Redelivered, then a no-op | Local chaos test |
| Redis frozen | Worker retries; no loss | Local chaos test |
| Aurora writer failover | Slot survives, Debezium resumes by LSN, no epoch change | AWS, 6 runs with writes |
| Aurora writer crash | Task may be killed; watchdog restarts it when safe | AWS, 3 runs (first one failed, section 7) |
| Full Aurora stop and start | Slot survived, recovered unaided | AWS, once |
| Slot missing or lost | Alert; epoch runbook | Unit only |
| Stale-epoch worker | Refuses to start | AWS and local |
| Restore / PITR / rebuild | Epoch runbook (new slot, new topic, epoch+1) | Runbook exercised, not a real restore |

## 7. What testing changed

1. **Debezium's poll interval is paid twice** (idle sleep, then queue hand-off). 500 -> 50 ms,
   plus a 20 ms worker batch wait, took median lag from about 630 ms to 80 ms.
2. **Debezium notices a dead connection only when it next sends.** `status.update.interval.ms`
   10 s -> 1 s cut detection from 6-9 s to 0.1-0.5 s, and worst staleness in a failover from
   17.5 s to 2.2-3.1 s.
3. **The 1 s restart wait introduced a worse failure.** In a writer crash, Debezium's restart
   hit "connection refused" while Aurora was still recovering. An error *during* task start
   is fatal, and Kafka Connect never restarts a failed task: 6,263 of 9,604 keys were still
   stale 14 minutes later. The default 10 s wait would only make this less likely. The
   watchdog is the fix; it recovered all 9,604 keys of that same run, so nothing was lost.
   With it, two further crashes passed, worst staleness 1.9 and 2.2 s.
4. **`max_slot_wal_keep_size` is instance-level on Aurora PG 16**, so it must be set on every
   instance to survive a failover.
5. **Burstable instances without credits** were throttled to baseline and failed Kafka's
   health check. Benchmarks on throttled nodes measure the throttle.

## 8. Results

us-east-2, Aurora PostgreSQL 16.13 Serverless v2, Valkey 8.2, t4g.large + t4g.medium.
Every soak inserts each key once with at most one later change, so a lost final event cannot
be masked; keys are compared with Aurora one by one in both caches.

| Test | Keys | Lost or different, A / B | Worst staleness A / B |
|---|---|---|---|
| Failover, default settings | 9,604 | 0 / 0 | 17.5 / 17.6 s |
| Failover, first tuning | 9,604 | 0 / 0 | 10.8 / 10.8 s |
| Failover, final settings, 4 runs | 9,604 each | 0 / 0 | 2.2-3.1 s |
| Crash, no watchdog | 9,604 | 6,263 stale until repaired, then 0 / 0 | over 14 min |
| Crash, watchdog, 2 runs | 7,204 each | 0 / 0 | 1.9, 2.2 s |
| 200 inserts/s, 2 min, no fault | 24,020 | 0 / 0 | 0.3 s |

Median lag with the final settings: 80-84 ms on A, 82-90 ms on B, including the 200 inserts/s run. Aurora kept every
acknowledged insert in every run. Writes paused 1.4-2.3 s in failovers and about 12 s in
crashes, which is Aurora's recovery time.

"Staleness" is commit acknowledged to first seen in the cache, for writes that were
acknowledged. It does not include the time the database refused writes.

## 9. Limits of the evidence

- 40 and 200 inserts/s for minutes, on small burstable nodes. Not a capacity test.
- Faults were requested failovers and injected instance crashes. No AZ loss, no network
  partition, no real restore.
- One Aurora version, one Region. The two-Region path was never applied and its plan never completed.
- Two crashes with the watchdog is a small sample.
- One failure of the epoch runbook in local testing was never conclusively explained. A
  stress test later reproduced a *different* failure in the same runbook (registration while
  the database was coming back), which is fixed; 9 of 9 bumps passed after that.
- A 20 s delay added for a Secrets Manager read-after-write race has not been exercised by a
  fresh deployment.
- Kafka and Connect are single nodes with replication factor 1. Losing the Kafka disk means
  the epoch runbook.

## 10. Security

Private subnets, no public IPs, no SSH (SSM only), customer-managed KMS keys, TLS to Aurora
(`verify-full`) and to the caches, per-role database users with no superuser. Database and
cache passwords are write-only in Terraform and never enter its state. Debezium's stored
configuration holds a reference, not the password.

Known trade-off: the throwaway Kafka CA and leaf keys **are** in Terraform state, so state
must be kept out of shared or synced folders (`scripts/tf.sh` does this).

## 11. Operations

- **Alert on:** slot missing, `wal_status` `lost`/`unreserved`, retained WAL, slot inactive,
  watchdog `alert`, DLQ growth.
- **Epoch runbook:** stop workers, remove the old connector and slot, new slot and topic
  prefix, start workers at epoch+1 (`cdcctl.sh bump-epoch`, safe to rerun).
- **Approximate idle cost** at list prices: 0.42 USD/hour for the whole test stack.

## 12. Open questions

1. Where does small-batch latency tuning start to cost throughput? Coalescing matters more
   as load grows.
2. Should the watchdog also page when it restarts, so repeated restarts are not silent?
3. Is a heartbeat table needed so the slot advances when captured tables are quiet?
4. Multi-AZ Kafka (for example MSK) before any production use.
