"""Redis client wrapper: every cache write goes through the version-gated Lua script."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

LUA_PATH = Path(__file__).with_name("update_if_newer.lua")
EPOCH_KEY = "cdc:epoch"


class EpochRegression(RuntimeError):
    """Worker configured with an epoch older than one already used against this cache."""


@dataclass(frozen=True)
class CacheEntry:
    epoch: int
    lsn: int
    deleted: bool
    fill: bool                       # populated by a reader, not by CDC
    payload: dict[str, Any] | None


class Invalidator:
    def __init__(
        self,
        client: redis.Redis | None = None,
        key_prefix: str = "product:",
        ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "3600")),
        fill_ttl_seconds: int = int(os.getenv("FILL_TTL_SECONDS", "60")),
        epoch: int = int(os.getenv("CDC_EPOCH", "0")),
    ) -> None:
        self.r = client or redis.Redis(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            # local-dev default matches docker-compose.yml; override in any real environment
            password=os.getenv("REDIS_PASSWORD", "cdc-local-dev") or None,
            ssl=os.getenv("REDIS_TLS", "false").lower() == "true",
            decode_responses=True,
            # Without timeouts a hung Redis blocks the worker forever instead of
            # raising TimeoutError into the retry loop.
            socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT_S", "5")),
            socket_connect_timeout=float(os.getenv("REDIS_CONNECT_TIMEOUT_S", "5")),
            # Exactly one retry layer: the worker's (bounded, backed off, counted in
            # metrics). redis-py >= 6 retries internally by default (10x in 8.x);
            # stacked with the worker's loop that is up to 100 attempts x 5 s,
            # enough to blow max.poll.interval.ms and get evicted from the group.
            retry=Retry(NoBackoff(), 0),
        )
        if epoch < 0:
            raise ValueError("epoch must be >= 0")
        self.key_prefix = key_prefix
        self.watermark_key = f"cdc:truncate:{key_prefix.rstrip(':')}"
        self.ttl_seconds = ttl_seconds
        # Short TTL on reader fills bounds the one residual race (see README).
        self.fill_ttl_seconds = fill_ttl_seconds
        self.epoch = epoch
        # register_script uses EVALSHA and transparently reloads on NOSCRIPT
        self._script = self.r.register_script(LUA_PATH.read_text(encoding="utf-8"))

    def key(self, pk: Any) -> str:
        return f"{self.key_prefix}{pk}"

    # ------------------------------------------------------------------ epoch
    def ensure_epoch(self) -> None:
        """Refuse to run with an epoch lower than one this cache has already seen.

        A regressed epoch would make every event lose to existing entries,
        silently freezing the cache.
        """
        stored = self.r.get(EPOCH_KEY)
        if stored is not None and int(stored) > self.epoch:
            raise EpochRegression(f"cache is at epoch {stored}, worker configured with {self.epoch}")
        if stored is None or int(stored) < self.epoch:
            self.r.set(EPOCH_KEY, self.epoch)

    # ------------------------------------------------------------- CDC writes
    def upsert(self, pk: Any, row: dict[str, Any], lsn: int) -> bool:
        return self._apply(self.key(pk), "set", _dump(row), lsn, self.ttl_seconds)

    def delete(self, pk: Any, lsn: int) -> bool:
        return self._apply(self.key(pk), "del", "", lsn, self.ttl_seconds)

    def truncate(self, lsn: int) -> bool:
        """Invalidate every entry of the table at or below (epoch, lsn)."""
        return self._apply(self.watermark_key, "truncate", "", lsn, 0)

    def apply_batch(self, changes: list[tuple[str, Any, dict[str, Any] | None, int]],
                    chunk: int = 1000) -> list[bool]:
        """Apply (op, pk, row, lsn) changes with op in c/u/r/d/t, pipelined.

        Not a transaction: each script call is atomic on its own, and the calls
        commute, so partial failure + full retry is safe."""
        results: list[bool] = []
        for i in range(0, len(changes), chunk):
            pipe = self.r.pipeline(transaction=False)
            for op, pk, row, lsn in changes[i:i + chunk]:
                if lsn is None or int(lsn) < 0:
                    raise ValueError(f"invalid lsn: {lsn!r}")
                if op == "t":
                    key, lua_op, payload, ttl = self.watermark_key, "truncate", "", 0
                elif op == "d":
                    key, lua_op, payload, ttl = self.key(pk), "del", "", self.ttl_seconds
                else:
                    key, lua_op, payload, ttl = self.key(pk), "set", _dump(row or {}), self.ttl_seconds
                self._script(keys=[key, self.watermark_key],
                             args=[lua_op, payload, str(self.epoch), str(int(lsn)), ttl], client=pipe)
            results.extend(r == 1 for r in pipe.execute())
        return results

    def _apply(self, key: str, op: str, payload: str, lsn: int, ttl: int) -> bool:
        if lsn is None or int(lsn) < 0:
            raise ValueError(f"invalid lsn: {lsn!r}")
        res = self._script(keys=[key, self.watermark_key],
                           args=[op, payload, str(self.epoch), str(int(lsn)), ttl])
        return res == 1

    # -------------------------------------------------------------- read path
    def fill(self, pk: Any, row: dict[str, Any]) -> bool:
        """Populate the cache from a database read, only if no valid entry exists.

        The entry is stamped below every real CDC version, so any change event
        for this key (already in flight or future) supersedes it.
        """
        res = self._script(keys=[self.key(pk), self.watermark_key],
                           args=["fill", _dump(row), str(self.epoch), "0", self.fill_ttl_seconds])
        return res == 1

    def get(self, pk: Any) -> CacheEntry | None:
        """Return the valid cache entry, or None. Tombstones are returned (deleted=True)."""
        pipe = self.r.pipeline(transaction=True)
        pipe.hgetall(self.key(pk))
        pipe.hmget(self.watermark_key, "epoch", "lsn")
        h, (wm_e, wm_l) = pipe.execute()
        if not h:
            return None
        e = CacheEntry(
            epoch=int(h.get("epoch", 0)),
            lsn=int(h["lsn"]),
            deleted=h.get("deleted") == "1",
            fill=h.get("fill") == "1",
            payload=json.loads(h["payload"]) if "payload" in h else None,
        )
        if wm_l is not None:
            wm = (int(wm_e or 0), int(wm_l))
            v = (e.epoch, e.lsn)
            if not (v > wm or (e.fill and v == wm)):
                return None  # written before a TRUNCATE
        return e

    def read_through(self, pk: Any, loader: Callable[[Any], dict[str, Any] | None]) -> dict[str, Any] | None:
        """Cache-aside read. `loader(pk)` must return the row in the same JSON shape
        Debezium emits (e.g. NUMERIC as string, timestamps as ISO-8601 UTC), or None."""
        e = self.get(pk)
        if e is not None:
            return None if e.deleted else e.payload
        row = loader(pk)
        if row is not None:
            self.fill(pk, row)
        return row


def _dump(row: dict[str, Any]) -> str:
    return json.dumps(row, separators=(",", ":"), sort_keys=True)
