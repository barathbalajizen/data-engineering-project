-- Data quality scorecard. audit.dq_log gets where a check comes from and what it measures; the views score
-- every run. Additive: existing rows are kept and only classified. Applied by src/migrate.py.
ALTER TABLE audit.dq_log ADD COLUMN IF NOT EXISTS check_source TEXT;   -- checks (src/checks.py) or dbt
ALTER TABLE audit.dq_log ADD COLUMN IF NOT EXISTS category     TEXT;   -- reconciliation, freshness, ...
CREATE INDEX IF NOT EXISTS ix_dq_log_run_id ON audit.dq_log (run_id);

-- Classify rows written before these columns existed (all came from src/checks.py)
UPDATE audit.dq_log SET check_source = 'checks' WHERE check_source IS NULL;
UPDATE audit.dq_log SET category = CASE
        WHEN check_name LIKE 'recon_%'     THEN 'reconciliation'
        WHEN check_name LIKE '%freshness%' THEN 'freshness'
        WHEN check_name LIKE '%unique%' OR check_name LIKE '%duplicate%' THEN 'uniqueness'
        WHEN check_name LIKE '%null%'      THEN 'completeness'
        ELSE 'business_rule' END
    WHERE category IS NULL;

-- Latest result per check per run (a retried dbt test is recorded again; the newest row wins)
CREATE OR REPLACE VIEW audit.dq_latest AS
SELECT DISTINCT ON (run_id, check_name) *
FROM audit.dq_log
ORDER BY run_id, check_name, id DESC;

-- One row per pipeline run. pass_rate counts only PASS; health_score counts a WARN as half a pass.
CREATE OR REPLACE VIEW audit.dq_scorecard AS
SELECT run_id,
       min(run_ts)                                    AS run_ts,
       count(*)                                       AS checks,
       count(*) FILTER (WHERE status = 'PASS')        AS passed,
       count(*) FILTER (WHERE status = 'WARN')        AS warned,
       count(*) FILTER (WHERE status = 'FAIL')        AS failed,
       round(100.0 * count(*) FILTER (WHERE status = 'PASS') / nullif(count(*), 0), 1) AS pass_rate,
       round(100.0 * (count(*) FILTER (WHERE status = 'PASS')
                      + 0.5 * count(*) FILTER (WHERE status = 'WARN')) / nullif(count(*), 0), 1) AS health_score
FROM audit.dq_latest
GROUP BY run_id;

-- Same, per category (reconciliation, freshness, uniqueness, completeness, validity, ...)
CREATE OR REPLACE VIEW audit.dq_scorecard_by_category AS
SELECT run_id,
       coalesce(category, 'other')                    AS category,
       min(run_ts)                                    AS run_ts,
       count(*)                                       AS checks,
       count(*) FILTER (WHERE status = 'PASS')        AS passed,
       count(*) FILTER (WHERE status = 'WARN')        AS warned,
       count(*) FILTER (WHERE status = 'FAIL')        AS failed,
       round(100.0 * count(*) FILTER (WHERE status = 'PASS') / nullif(count(*), 0), 1) AS pass_rate
FROM audit.dq_latest
GROUP BY run_id, coalesce(category, 'other');
