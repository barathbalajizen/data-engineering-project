-- Silver incremental checkpoint: the last Bronze Delta version whose changes (Change Data Feed) were merged
-- into Silver. Updated only after the Silver MERGE and quarantine write succeed, so a crash in between just
-- reprocesses the same (idempotent) range. Delete a row to force a full rebuild of that table.
CREATE TABLE IF NOT EXISTS control.silver_checkpoint (
    table_name      TEXT PRIMARY KEY,
    bronze_version  BIGINT    NOT NULL,
    mode            TEXT      NOT NULL,           -- full / incremental: how the last run read Bronze
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);
