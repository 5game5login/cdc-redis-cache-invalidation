"""Idempotent schema/role/publication setup. Works on Aurora and on plain Postgres.

Aurora has no REPLICATION attribute for non-superusers: membership of the
rds_replication role grants it instead. Run with the admin DSN.
"""
import os

import psycopg
from psycopg import sql

DDL = """
CREATE TABLE IF NOT EXISTS public.products (
    id          SERIAL PRIMARY KEY,
    name        TEXT           NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    stock       INTEGER        NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);
ALTER TABLE public.products REPLICA IDENTITY DEFAULT;
"""

ROLES = {   # role -> env var holding its password
    "debezium": "DEBEZIUM_PASSWORD",
    "cdc_monitor": "MONITOR_PASSWORD",
    "cdc_app": "APP_PASSWORD",
}


def main() -> None:
    with psycopg.connect(os.environ["ADMIN_DSN"], autocommit=True) as c:
        c.execute(DDL)
        for role, env in ROLES.items():
            exists = c.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
            verb = "ALTER" if exists else "CREATE"
            c.execute(sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(os.environ[env])))
        db = c.execute("SELECT current_database()").fetchone()[0]
        for role in ROLES:
            c.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(db), sql.Identifier(role)))
        on_rds = c.execute("SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication'").fetchone()
        c.execute("GRANT rds_replication TO debezium" if on_rds else "ALTER ROLE debezium REPLICATION")
        c.execute("GRANT USAGE ON SCHEMA public TO debezium, cdc_app")
        c.execute("GRANT SELECT ON public.products TO debezium")
        c.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON public.products TO cdc_app")
        c.execute("GRANT USAGE ON SEQUENCE public.products_id_seq TO cdc_app")
        c.execute("GRANT pg_monitor TO cdc_monitor")
        if not c.execute("SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'").fetchone():
            c.execute("CREATE PUBLICATION dbz_publication FOR TABLE public.products "
                      "WITH (publish = 'insert, update, delete, truncate')")
        if not c.execute("SELECT 1 FROM public.products LIMIT 1").fetchone():
            c.execute("INSERT INTO public.products (name, price, stock) VALUES "
                      "('Widget', 9.99, 100), ('Gadget', 24.50, 40), ('Doohickey', 3.75, 500)")
        wal = c.execute("SHOW wal_level").fetchone()[0]
        keep = c.execute("SHOW max_slot_wal_keep_size").fetchone()[0]
        ssl = c.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()[0]
        print(f"OK wal_level={wal} max_slot_wal_keep_size={keep} ssl={ssl} rds={bool(on_rds)}")
        if wal != "logical":
            raise SystemExit("wal_level is not 'logical'")


if __name__ == "__main__":
    main()
