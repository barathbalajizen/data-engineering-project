-- Runs once, the first time the Postgres volume is created.
CREATE SCHEMA IF NOT EXISTS source;     -- simulated operational (OLTP) system
CREATE SCHEMA IF NOT EXISTS staging;    -- silver layer loaded for dbt
CREATE SCHEMA IF NOT EXISTS control;    -- pipeline metadata (watermarks)
CREATE SCHEMA IF NOT EXISTS audit;      -- data quality results
CREATE SCHEMA IF NOT EXISTS snapshots;  -- dbt SCD2 snapshots
CREATE SCHEMA IF NOT EXISTS analytics;  -- gold layer (star schema)

CREATE TABLE IF NOT EXISTS control.watermark (
    table_name     TEXT PRIMARY KEY,
    last_watermark TIMESTAMP NOT NULL DEFAULT '1900-01-01',
    updated_at     TIMESTAMP DEFAULT now()
);
INSERT INTO control.watermark (table_name) VALUES ('orders') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS audit.dq_log (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT,
    check_name  TEXT,
    status      TEXT,       -- PASS / FAIL / WARN
    rows_failed BIGINT,
    detail      TEXT,
    run_ts      TIMESTAMP DEFAULT now()
);
