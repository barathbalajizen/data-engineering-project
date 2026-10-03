-- Rejected-records report: per run, table and validation rule, how many rows Silver quarantined and a few
-- sample keys. The quarantined rows themselves stay in Delta (lake/silver/_quarantine/<table>); this table
-- makes the report queryable from Postgres (dashboard, alerts) without Spark. Applied by src/migrate.py.
-- incremental tables (orders): rows rejected in this run's batch; full-snapshot tables: the current invalid set.
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_run_id  TEXT      NOT NULL,
    batch_id         TEXT,
    table_name       TEXT      NOT NULL,
    rule             TEXT      NOT NULL,
    rows_rejected    BIGINT    NOT NULL,
    sample_keys      TEXT,                       -- e.g. order_id=o4; order_id=o9
    recorded_at      TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_rejected_records_run   ON audit.rejected_records (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_rejected_records_table ON audit.rejected_records (table_name, recorded_at);
