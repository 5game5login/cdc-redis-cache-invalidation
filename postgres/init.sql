-- Schema for the CDC demo
CREATE TABLE IF NOT EXISTS public.products (
    id          SERIAL PRIMARY KEY,
    name        TEXT           NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    stock       INTEGER        NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY DEFAULT (primary key) is sufficient: deletes only need the
-- PK in the before-image, and update before-images are never used. FULL would
-- write the entire old row into WAL on every UPDATE/DELETE for no benefit.
ALTER TABLE public.products REPLICA IDENTITY DEFAULT;

-- Publication consumed by Debezium (pgoutput plugin); includes TRUNCATE
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR TABLE public.products
    WITH (publish = 'insert, update, delete, truncate');

-- Least-privilege roles (local-dev passwords; use a secrets manager elsewhere)
-- Debezium: replication + read access to captured tables only. No superuser.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE inventory TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.products TO debezium;

-- Worker slot monitor: read-only monitoring views only.
CREATE ROLE cdc_monitor WITH LOGIN PASSWORD 'cdc_monitor';
GRANT CONNECT ON DATABASE inventory TO cdc_monitor;
GRANT pg_monitor TO cdc_monitor;

-- Seed data (captured by Debezium's initial snapshot as op='r')
INSERT INTO public.products (name, price, stock) VALUES
    ('Widget',   9.99, 100),
    ('Gadget',  24.50,  40),
    ('Doohickey', 3.75, 500);
