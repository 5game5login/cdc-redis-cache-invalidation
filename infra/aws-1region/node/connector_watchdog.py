"""Restart a FAILED Debezium task, but only when that cannot skip events.

Why: Debezium retries a lost connection, but if the database refuses the connection
*while the task is restarting*, the task is killed and stays FAILED until someone
restarts it (seen when an Aurora writer crash took 11 s to recover).

Why not restart blindly: if the replication slot is missing, Debezium can create a new
one at the current WAL position, silently skipping everything in between. So a restart
needs a read-write database where the slot exists and is not lost. Anything else is an
alert for the epoch runbook, never an automatic action.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request

log = logging.getLogger("cdc.watchdog")


def decide(task_states: list[str], db: dict | None) -> tuple[str, str]:
    """Pure decision. `db` is None if unreachable, else {read_write, slot, wal_status}.

    Returns (action, reason) with action in ok | wait | restart | alert."""
    if "FAILED" not in task_states:
        return "ok", ""
    if db is None:
        return "wait", "task failed; database unreachable"
    if not db["read_write"]:
        return "wait", "task failed; connected to a read-only instance"
    if not db["slot"]:
        return "alert", "task failed and the replication slot is missing: run the epoch runbook"
    if db["wal_status"] in ("lost", "unreserved"):
        return "alert", f"task failed and the slot is {db['wal_status']}: run the epoch runbook"
    return "restart", "task failed; database is read-write and the slot is intact"


def connector_states(url: str, name: str) -> list[str]:
    with urllib.request.urlopen(f"{url}/connectors/{name}/status", timeout=5) as r:
        s = json.load(r)
    return [s["connector"]["state"]] + [t["state"] for t in s["tasks"]]


def probe_db(dsn: str, slot: str) -> dict | None:
    import psycopg
    try:
        with psycopg.connect(dsn, connect_timeout=3, autocommit=True) as c:
            ro = c.execute("SELECT pg_is_in_recovery()").fetchone()[0]
            row = c.execute("SELECT wal_status FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()
        return {"read_write": not ro, "slot": row is not None, "wal_status": row[0] if row else None}
    except psycopg.Error:
        return None


def restart(url: str, name: str) -> None:
    req = urllib.request.Request(f"{url}/connectors/{name}/restart?includeTasks=true&onlyFailed=true", method="POST")
    urllib.request.urlopen(req, timeout=15).close()


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    url, name = os.environ["CONNECT_URL"], os.environ["CONNECTOR_NAME"]
    dsn, slot = os.environ["PG_DSN"], os.environ["SLOT_NAME"]
    interval, last, backoff, not_before = float(os.getenv("WATCHDOG_INTERVAL_S", "1")), "", 2.0, 0.0
    log.info("watching %s (slot %s)", name, slot)
    while True:
        time.sleep(interval)
        try:
            states = connector_states(url, name)
        except Exception as e:      # Connect itself is down or rebalancing
            states, why = [], f"Connect not answering: {e}"
            if why != last:
                log.warning(why)
                last = why
            continue
        action, reason = decide(states, probe_db(dsn, slot) if "FAILED" in states else None)
        if reason != last:
            {"alert": log.critical, "wait": log.warning}.get(action, log.info)("%s: %s", action, reason or "healthy")
            last = reason
        if action == "ok":
            backoff = 2.0
        elif action == "restart" and time.monotonic() >= not_before:
            try:
                restart(url, name)
                log.warning("restarted %s", name)
            except Exception as e:
                log.warning("restart request failed: %s", e)
            not_before = time.monotonic() + backoff
            backoff = min(backoff * 2, 60.0)    # a task that keeps failing must not be hammered


if __name__ == "__main__":
    main()
