"""Flows you run from the Prefect UI (Deployments > <name> > Run > Custom run) instead of the terminal:
data setup, source-change simulation, health checks and demos. Each one wraps a script in src/."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from prefect import flow, task  # noqa: E402

from ecommerce_flows import (HOOKS, PY, SRC, flow_audit, migrate_db,  # noqa: E402
                             publish_run_summary, run_cmd, run_pipeline_steps)


@task(name="generate-sample-data", retries=0)
def generate_sample_data(n_orders: int):
    run_cmd([PY, f"{SRC}/generate_sample_data.py", str(n_orders)])


@task(name="load-source", retries=2, retry_delay_seconds=30)
def load_source():
    run_cmd([PY, f"{SRC}/load_source.py"])


@task(name="simulate-changes", retries=0)
def simulate_changes():
    run_cmd([PY, f"{SRC}/simulate_changes.py"])


@task(name="bronze-stats", retries=0)
def bronze_stats():
    run_cmd([PY, f"{SRC}/bronze_stats.py"])


@task(name="simulate-bronze-loss", retries=0)
def simulate_bronze_loss(start: str, end: str):
    run_cmd([PY, f"{SRC}/simulate_bronze_loss.py", "--start", start, "--end", end])


@task(name="export-showcase", retries=2, retry_delay_seconds=30)
def export_showcase():
    run_cmd([PY, f"{SRC}/export_showcase.py"])


@task(name="delta-inspect", retries=0)
def delta_inspect(action: str, table: str, version: int | None, timestamp: str | None,
                  from_version: int | None, to_version: int | None, limit: int):
    cmd = [PY, f"{SRC}/delta_tools.py", action, "--table", table, "--limit", str(limit)]
    for flag, value in (("--version", version), ("--timestamp", timestamp),
                        ("--from-version", from_version), ("--to-version", to_version)):
        if value is not None:
            cmd += [flag, str(value)]
    run_cmd(cmd)


@task(name="lake-maintenance", retries=1, retry_delay_seconds=60)
def lake_maintenance(min_files: int, small_file_mb: float, vacuum: bool):
    cmd = [PY, f"{SRC}/maintenance.py", "--min-files", str(min_files), "--small-file-mb", str(small_file_mb)]
    run_cmd(cmd + (["--vacuum"] if vacuum else []), timeout=3 * 3600)


@task(name="skew-demo", retries=0)
def skew_demo(rows: int, hot_share: float, salts: int):
    run_cmd([PY, f"{SRC}/skew_demo.py", "--rows", str(rows), "--hot-share", str(hot_share),
             "--salts", str(salts)], timeout=3 * 3600)


@flow(name="ecommerce-setup", **HOOKS)
def setup_demo_data(n_orders: int = 20000, generate_csvs: bool = True, run_pipeline_after: bool = True):
    """First run: create sample CSVs (or use your own Olist CSVs in data/raw), load them into the Postgres
    source, then optionally run the whole pipeline once. WARNING: replaces the source tables and resets the
    orders watermark, so only use it for the first setup or a clean restart."""
    migrate_db()
    try:
        with flow_audit():
            if generate_csvs:
                generate_sample_data(n_orders)
            load_source()
            if run_pipeline_after:
                run_pipeline_steps()
    finally:
        if run_pipeline_after:
            publish_run_summary("setup")


@flow(name="ecommerce-simulate-changes", **HOOKS)
def simulate_source_changes():
    """Simulate one day of source activity (200 orders delivered, 100 customers move, 500 new orders).
    Then run ecommerce-daily to see the incremental load and SCD Type 2 history."""
    simulate_changes()


@flow(name="ecommerce-bronze-health", **HOOKS)
def bronze_health():
    """Log Bronze / Silver / quarantine row counts and the number of duplicate order versions (should be 0)."""
    bronze_stats()


@flow(name="ecommerce-simulate-bronze-loss", **HOOKS)
def bronze_loss(start: str, end: str):
    """Delete orders with updated_at in [start, end) from Bronze, to practise recovery with ecommerce-backfill."""
    simulate_bronze_loss(start, end)


@flow(name="ecommerce-skew-demo", **HOOKS)
def skew_join_demo(rows: int = 1_000_000, hot_share: float = 0.6, salts: int = 16):
    """Data-skew demo: the same join run naive, broadcast, salted and with Spark AQE. The comparison table
    is in the task log. Use a smaller `rows` if the container runs out of memory."""
    skew_demo(rows, hot_share, salts)


@flow(name="ecommerce-export-showcase", **HOOKS)
def export_showcase_flow():
    """Export a snapshot of the Gold results to docs/sample_output/ (CSVs + README.md). Commit that folder so
    people browsing the repo on GitHub can see the output without running the pipeline."""
    export_showcase()


@flow(name="ecommerce-delta-inspect", **HOOKS)
def delta_inspect_flow(action: str = "history", table: str = "bronze/orders", version: int | None = None,
                       timestamp: str | None = None, from_version: int | None = None,
                       to_version: int | None = None, limit: int = 10):
    """Read-only Delta inspection (output in the task log):
    history  - versions, operations and row counts (and the table's retention/CDF properties)
    as-of    - time travel: compare `version` or `timestamp` with the current table
    changes  - Change Data Feed from `from_version` (to `to_version`); CDF is enabled on bronze/orders"""
    delta_inspect(action, table, version, timestamp, from_version, to_version, limit)


@flow(name="ecommerce-lake-maintenance", **HOOKS)
def lake_maintenance_flow(min_files: int = 16, small_file_mb: float = 32, vacuum: bool = False):
    """Compact Delta tables that have accumulated many small files (OPTIMIZE) and report what VACUUM would
    delete. vacuum=true really deletes unreferenced files older than each table's retention (7 days), which
    also ends time travel to versions older than that. Safe to run any time; one flow run at a time."""
    migrate_db()
    with flow_audit():
        lake_maintenance(min_files, small_file_mb, vacuum)
