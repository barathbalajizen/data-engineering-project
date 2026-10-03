-- Checkpoint history and execution-history views. Additive; applied by src/migrate.py.

-- Every change of a watermark or Silver checkpoint, written by a trigger, so it cannot be bypassed by code
-- paths that forget to log (including manual UPDATEs). The writer sets `app.run_id` for the transaction
-- (src/checkpoints.py) so each change is linked to the pipeline run that made it.
CREATE TABLE IF NOT EXISTS control.checkpoint_history (
    id               BIGSERIAL PRIMARY KEY,
    checkpoint       TEXT      NOT NULL,            -- watermark / silver_checkpoint
    table_name       TEXT      NOT NULL,
    old_value        TEXT,
    new_value        TEXT,
    pipeline_run_id  TEXT,                          -- NULL for manual changes
    changed_at       TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_checkpoint_history ON control.checkpoint_history (checkpoint, table_name, changed_at);

CREATE OR REPLACE FUNCTION control.log_checkpoint_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    old_v TEXT := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ->> TG_ARGV[0] END;
    new_v TEXT := to_jsonb(NEW) ->> TG_ARGV[0];
BEGIN
    IF old_v IS DISTINCT FROM new_v THEN
        INSERT INTO control.checkpoint_history (checkpoint, table_name, old_value, new_value, pipeline_run_id)
        VALUES (TG_TABLE_NAME, NEW.table_name, old_v, new_v, nullif(current_setting('app.run_id', true), ''));
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_watermark_history ON control.watermark;
CREATE TRIGGER trg_watermark_history AFTER INSERT OR UPDATE ON control.watermark
    FOR EACH ROW EXECUTE FUNCTION control.log_checkpoint_change('last_watermark');

DROP TRIGGER IF EXISTS trg_silver_checkpoint_history ON control.silver_checkpoint;
CREATE TRIGGER trg_silver_checkpoint_history AFTER INSERT OR UPDATE ON control.silver_checkpoint
    FOR EACH ROW EXECUTE FUNCTION control.log_checkpoint_change('bronze_version');

-- One row per flow run: status, duration, task attempts, retries, first failed task and rows moved.
CREATE OR REPLACE VIEW audit.v_flow_runs AS
SELECT f.pipeline_run_id,
       f.task_name                                       AS flow_name,
       f.status,
       f.start_ts,
       f.end_ts,
       f.duration_seconds,
       coalesce(t.task_attempts, 0)                      AS task_attempts,
       coalesce(t.failed_attempts, 0)                    AS failed_attempts,
       coalesce(t.retried_attempts, 0)                   AS retried_attempts,
       t.first_failed_task,
       t.steps_succeeded,
       coalesce(r.rows_inserted, 0)                      AS rows_inserted,
       coalesce(r.rows_updated, 0)                       AS rows_updated,
       coalesce(r.rows_rejected, 0)                      AS rows_rejected,
       f.error_details
FROM audit.pipeline_run_log f
LEFT JOIN (
    SELECT pipeline_run_id,
           count(*)                                                  AS task_attempts,
           count(*) FILTER (WHERE status IN ('FAILED', 'ABANDONED')) AS failed_attempts,
           count(*) FILTER (WHERE retry_count > 0)                   AS retried_attempts,
           (array_agg(task_name ORDER BY id) FILTER (WHERE status IN ('FAILED', 'ABANDONED')))[1]
                                                                     AS first_failed_task,
           string_agg(task_name, ' > ' ORDER BY id) FILTER (WHERE status = 'SUCCESS') AS steps_succeeded
    FROM audit.pipeline_run_log WHERE level = 'task' GROUP BY pipeline_run_id
) t ON t.pipeline_run_id = f.pipeline_run_id
LEFT JOIN (
    SELECT pipeline_run_id, sum(inserted_count) AS rows_inserted, sum(updated_count) AS rows_updated,
           sum(rejected_count) AS rows_rejected
    FROM audit.pipeline_run_log WHERE level = 'table' GROUP BY pipeline_run_id
) r ON r.pipeline_run_id = f.pipeline_run_id
WHERE f.level = 'flow';

-- One row per task: attempts, success rate, duration statistics of successful attempts, last result.
CREATE OR REPLACE VIEW audit.v_task_stats AS
SELECT task_name,
       count(*)                                                       AS attempts,
       count(*) FILTER (WHERE status = 'SUCCESS')                     AS successes,
       count(*) FILTER (WHERE status IN ('FAILED', 'ABANDONED'))      AS failures,
       round(100.0 * count(*) FILTER (WHERE status = 'SUCCESS') / count(*), 1) AS success_rate,
       round(avg(duration_seconds) FILTER (WHERE status = 'SUCCESS'), 1)       AS avg_seconds,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds)
              FILTER (WHERE status = 'SUCCESS'))::numeric, 1)                  AS median_seconds,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds)
              FILTER (WHERE status = 'SUCCESS'))::numeric, 1)                  AS p95_seconds,
       round(max(duration_seconds) FILTER (WHERE status = 'SUCCESS'), 1)       AS max_seconds,
       max(start_ts)                                                  AS last_start,
       (array_agg(status ORDER BY id DESC))[1]                        AS last_status
FROM audit.pipeline_run_log
WHERE level = 'task'
GROUP BY task_name;
