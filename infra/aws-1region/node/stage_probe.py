"""Split the invalidation lag of a soak run into pipeline stages.

Reads the run's events back from Kafka A and joins them with the writer's ack times and
each side's first-seen times:

  ack -> debezium    commit acknowledged -> Debezium decoded the event (envelope ts)
  debezium -> kafka  waiting in Debezium's queue / Connect producer (record CreateTime)
  kafka -> seen a/b  consumer + Lua (b also includes MirrorMaker, which keeps CreateTime)

`bursts` shows where events bunch up: the number of pauses longer than 50 ms between
consecutive events at that point, and the median length of those pauses.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from bench_lib import bursts, pctl


def events(base: int, top: int, since_ms: int) -> dict[int, tuple[float, float]]:
    """id -> (envelope time, kafka CreateTime) in seconds, for inserts only."""
    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "group.id": f"probe-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "enable.partition.eof": False})
    topic = os.environ["CDC_TOPIC"]
    parts = list(c.list_topics(topic, timeout=20).topics[topic].partitions)
    start = c.offsets_for_times([TopicPartition(topic, p, since_ms) for p in parts], timeout=20)
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=20)[1] for p in parts}
    start = [tp for tp in start if 0 <= tp.offset < ends[tp.partition]]
    c.assign(start)
    left = {tp.partition for tp in start}
    out: dict[int, tuple[float, float]] = {}
    deadline = time.time() + 90
    while left and time.time() < deadline:
        for m in c.consume(1000, timeout=1.0):
            if m.error():
                continue
            if m.offset() + 1 >= ends[m.partition()]:
                left.discard(m.partition())
            env = json.loads(m.value()) if m.value() else {}
            if env.get("op") == "c" and base <= env["after"]["id"] < top:
                ts = env["ts_us"] / 1e6 if "ts_us" in env else env["ts_ms"] / 1e3
                out.setdefault(env["after"]["id"], (ts, m.timestamp()[1] / 1e3))
    c.close()
    return out


def main() -> None:
    d = Path("/run/cdc") / sys.argv[1]
    plan = json.loads((d / "plan.json").read_text())
    w = json.loads((d / "writes.json").read_text())
    ack = {int(i): t for i, _, t in w["inserts"]}
    ev = events(plan["base"], plan["base"] + plan["max"], int((w["t0"] - 5) * 1000))
    seen = {}
    for s in "ab":
        f = d / f"watch-{s}.json"
        if f.exists():
            seen[s] = {int(i): t for i, _, t in json.loads(f.read_text())["seen"]}

    def stat(xs: list[float]) -> dict:
        return {"n": len(xs), "p50_ms": round(pctl(xs, 50) * 1000, 1), "p99_ms": round(pctl(xs, 99) * 1000, 1)}

    ids = [i for i in ev if i in ack]
    out = {
        "events": len(ids), "acked": len(ack),
        "ack_to_debezium": stat([ev[i][0] - ack[i] for i in ids]),
        "debezium_to_kafka": stat([ev[i][1] - ev[i][0] for i in ids]),
        "bursts": {"acks": bursts([ack[i] for i in ids]), "debezium": bursts([ev[i][0] for i in ids]),
                   "kafka": bursts([ev[i][1] for i in ids])},
    }
    for s, m in seen.items():
        both = [i for i in ids if i in m]
        out[f"kafka_to_seen_{s}"] = stat([m[i] - ev[i][1] for i in both])
        out[f"total_{s}"] = stat([m[i] - ack[i] for i in both])
        out["bursts"][f"cache_{s}"] = bursts([m[i] for i in both])
    print(json.dumps(out))


if __name__ == "__main__":
    main()
