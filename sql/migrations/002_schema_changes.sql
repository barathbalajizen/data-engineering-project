-- Schema drift log: every detected change between an incoming schema and the accepted target schema,
-- and what the pipeline did about it (evolved / cast / filled_null / rejected). Applied by src/migrate.py.
CREATE TABLE IF NOT EXISTS audit.schema_changes (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_run_id  TEXT      NOT NULL,
    task_name        TEXT,
    batch_id         TEXT,
    table_name       TEXT      NOT NULL,     -- target, e.g. bronze.orders or staging.orders
    change_type      TEXT      NOT NULL CHECK (change_type IN ('added', 'removed', 'type_changed')),
    column_name      TEXT      NOT NULL,
    old_type         TEXT,
    new_type         TEXT,
    action           TEXT      NOT NULL CHECK (action IN ('evolved', 'cast', 'filled_null', 'rejected')),
    detected_at      TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_schema_changes_table ON audit.schema_changes (table_name, detected_at);
