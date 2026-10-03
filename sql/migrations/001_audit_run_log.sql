-- Pipeline run audit and lineage (kept separate from audit.dq_log).
-- Additive and idempotent: safe to apply to an existing database. Applied by src/migrate.py.

-- One row per flow run (level='flow'), per task attempt (level='task') and per table processed in a task
-- (level='table'). A retried task gets one row per attempt; retry_count = attempt number - 1.
CREATE TABLE IF NOT EXISTS audit.pipeline_run_log (
    id                BIGSERIAL PRIMARY KEY,
    pipeline_run_id   TEXT        NOT NULL,          -- Prefect flow run id (or 'manual-...')
    flow_name         TEXT,
    task_name         TEXT        NOT NULL,
    level             TEXT        NOT NULL CHECK (level IN ('flow', 'task', 'table')),
    table_name        TEXT,                          -- e.g. bronze.orders (level='table' only)
    batch_id          TEXT,                          -- unique per table per attempt (level='table' only)
    start_ts          TIMESTAMP   NOT NULL DEFAULT clock_timestamp(),
    end_ts            TIMESTAMP,
    duration_seconds  NUMERIC(12, 3),
    source_row_count  BIGINT,
    inserted_count    BIGINT,
    updated_count     BIGINT,
    rejected_count    BIGINT,
    status            TEXT        NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'ABANDONED')),
    retry_count       INT         NOT NULL DEFAULT 0,
    error_details     TEXT
);
CREATE INDEX IF NOT EXISTS ix_run_log_run_id   ON audit.pipeline_run_log (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_run_log_start_ts ON audit.pipeline_run_log (start_ts);
CREATE INDEX IF NOT EXISTS ix_run_log_status   ON audit.pipeline_run_log (status) WHERE status <> 'SUCCESS';

-- Data lineage: which batch moved how many rows from which source object to which target object.
CREATE TABLE IF NOT EXISTS audit.lineage (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_run_id  TEXT      NOT NULL,
    task_name        TEXT,
    batch_id         TEXT      NOT NULL,
    source_object    TEXT      NOT NULL,             -- e.g. postgres:source.orders, delta:bronze/orders
    target_object    TEXT      NOT NULL,
    row_count        BIGINT,
    target_version   BIGINT,                         -- Delta table version written (time travel), NULL for Postgres
    detail           TEXT,                           -- e.g. the extract window
    created_at       TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_lineage_run_id ON audit.lineage (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_lineage_batch  ON audit.lineage (batch_id);
