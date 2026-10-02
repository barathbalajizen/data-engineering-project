"""Flows you run from the Prefect UI (Deployments > <name> > Run > Custom run) instead of the terminal:
data setup, source-change simulation, health checks and demos. Each one wraps a script in src/."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from prefect import flow, task  # noqa: E402

from ecommerce_flows import (PY, SRC, notify_failure, publish_run_summary,  # noqa: E402
                             run_cmd, run_pipeline_steps)

HOOKS = dict(on_failure=[notify_failure], on_crashed=[notify_failure])


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


@task(name="skew-demo", retries=0)
def skew_demo(rows: int, hot_share: float, salts: int):
    run_cmd([PY, f"{SRC}/skew_demo.py", "--rows", str(rows), "--hot-share", str(hot_share),
             "--salts", str(salts)], timeout=3 * 3600)


@flow(name="ecommerce-setup", **HOOKS)
def setup_demo_data(n_orders: int = 20000, generate_csvs: bool = True, run_pipeline_after: bool = True):
    """First run: create sample CSVs (or use your own Olist CSVs in data/raw), load them into the Postgres
    source, then optionally run the whole pipeline once. WARNING: replaces the source tables and resets the
    orders watermark, so only use it for the first setup or a clean restart."""
    try:
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
