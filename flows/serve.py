"""Registers all deployments with the Prefect server and keeps running to execute their runs.

  ecommerce-daily/daily                  scheduled (DAILY_CRON in SCHEDULE_TZ); also runnable manually
  ecommerce-backfill/backfill            manual: start, end, chunk_days, rebuild_downstream
  ecommerce-setup/run                    first-time data setup (+ optional full pipeline run)
  ecommerce-simulate-changes/run         simulate a day of source changes
  ecommerce-bronze-health/run            Bronze/Silver counts, duplicate check
  ecommerce-simulate-bronze-loss/run     delete a window from Bronze (backfill practice)
  ecommerce-skew-demo/run                data-skew demo
  ecommerce-export-showcase/run          export result snapshot to docs/sample_output (for GitHub)
  ecommerce-delta-inspect/run            Delta history, time travel and Change Data Feed (read-only)

limit=1: only one flow run executes at a time, so runs never overlap (the others wait their turn).
pause_on_shutdown=False: the schedule keeps running across container restarts.
"""
import os

from prefect import serve
from prefect.schedules import Cron

from ecommerce_flows import backfill_pipeline, daily_pipeline
from ops_flows import (bronze_health, bronze_loss, delta_inspect_flow, export_showcase_flow, setup_demo_data,
                       simulate_source_changes, skew_join_demo)

if __name__ == "__main__":
    tz = os.getenv("SCHEDULE_TZ", "Asia/Kolkata")
    cron = os.getenv("DAILY_CRON", "0 2 * * *")

    deployments = [
        daily_pipeline.to_deployment(name="daily", schedule=Cron(cron, timezone=tz),
                                     tags=["ecommerce", "scheduled"]),
        backfill_pipeline.to_deployment(name="backfill", tags=["ecommerce", "manual"]),
        setup_demo_data.to_deployment(name="run", tags=["ecommerce", "setup"]),
        simulate_source_changes.to_deployment(name="run", tags=["ecommerce", "demo"]),
        bronze_health.to_deployment(name="run", tags=["ecommerce", "demo"]),
        bronze_loss.to_deployment(name="run", tags=["ecommerce", "demo"]),
        skew_join_demo.to_deployment(name="run", tags=["ecommerce", "demo"]),
        export_showcase_flow.to_deployment(name="run", tags=["ecommerce", "demo"]),
        delta_inspect_flow.to_deployment(name="run", tags=["ecommerce", "ops"]),
    ]
    serve(*deployments, pause_on_shutdown=False, limit=1)
