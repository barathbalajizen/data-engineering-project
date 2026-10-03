-- Measured performance results (src/benchmark.py, src/benchmark_dbt.py). One row per case and variant;
-- seconds are wall-clock times of each repetition. Applied by src/migrate.py.
CREATE TABLE IF NOT EXISTS audit.benchmark_results (
    id              BIGSERIAL PRIMARY KEY,
    suite_run       TEXT      NOT NULL,          -- one benchmark session
    benchmark       TEXT      NOT NULL,          -- e.g. silver_orders, jdbc_write
    variant         TEXT      NOT NULL,          -- e.g. incremental, full, batchsize=10000
    scale           INT       NOT NULL,          -- 1 = current data volume, 10 = ten times
    rows_processed  BIGINT,
    runs            INT       NOT NULL,
    min_seconds     NUMERIC(12, 3) NOT NULL,
    median_seconds  NUMERIC(12, 3) NOT NULL,
    max_seconds     NUMERIC(12, 3) NOT NULL,
    details         JSONB,                       -- files, partitions, environment, ...
    measured_at     TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_benchmark_results ON audit.benchmark_results (suite_run, benchmark);
