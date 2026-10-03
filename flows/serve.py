"""Registers all deployments with the Prefect server and keeps running to execute their runs.

Deployment names say what they do and sort (A to Z in the Prefect UI) in the order a newcomer needs them:
numbered = the main pipeline, in the order to run it; ops- = operations; demo- = demos that change data on purpose.

  ecommerce-setup/01-first-time-setup                         generate + load source data, run the pipeline once
  ecommerce-daily/02-daily-incremental-load                   scheduled (DAILY_CRON in SCHEDULE_TZ); also manual
  ecommerce-backfill/03-backfill-date-range                   manual: start, end, chunk_days, rebuild_downstream
  ecommerce-export-showcase/04-export-dashboard-snapshot      export results to docs/sample_output (for GitHub)
  ecommerce-bronze-health/ops-layer-health-check              row counts per layer, duplicate check
  ecommerce-delta-inspect/ops-delta-time-travel-inspect       Delta history, time travel, Change Data Feed (read-only)
  ecommerce-lake-maintenance/ops-weekly-lake-maintenance      OPTIMIZE + VACUUM dry run (MAINTENANCE_CRON, Sun 03:00)
  ecommerce-simulate-changes/demo-simulate-source-changes     simulate a day of source changes
  ecommerce-simulate-bronze-loss/demo-simulate-bronze-data-loss  delete a window from Bronze (backfill practice)
  ecommerce-skew-demo/demo-data-skew-joins                    data-skew join comparison

limit=1: only one flow run executes at a time, so runs never overlap (the others wait their turn).
pause_on_shutdown=False: the schedule keeps running across container restarts.
"""
import os

from prefect import serve
from prefect.schedules import Cron

from ecommerce_flows import backfill_pipeline, daily_pipeline
from ops_flows import (
    bronze_health,
    bronze_loss,
    delta_inspect_flow,
    export_showcase_flow,
    lake_maintenance_flow,
    setup_demo_data,
    simulate_source_changes,
    skew_join_demo,
)

if __name__ == "__main__":
    tz = os.getenv("SCHEDULE_TZ", "Asia/Kolkata")
    cron = os.getenv("DAILY_CRON", "0 2 * * *")

    deployments = [
        setup_demo_data.to_deployment(
            name="01-first-time-setup", tags=["ecommerce", "setup"],
            description="Start here. Generates (or loads) the source data and runs the whole pipeline once."),
        daily_pipeline.to_deployment(
            name="02-daily-incremental-load", schedule=Cron(cron, timezone=tz), tags=["ecommerce", "scheduled"],
            description="Daily incremental load: Bronze > Silver > staging > dbt > quality checks. "
                        "start_from / stop_after run part of it; resume_failed continues the latest failed run."),
        backfill_pipeline.to_deployment(
            name="03-backfill-date-range", tags=["ecommerce", "manual"],
            description="Reprocess orders updated in [start, end) in chunks, then rebuild downstream. "
                        "The watermark is not touched."),
        export_showcase_flow.to_deployment(
            name="04-export-dashboard-snapshot", tags=["ecommerce", "ops"],
            description="Export the dashboard datasets to docs/sample_output so the results are visible on GitHub."),
        bronze_health.to_deployment(
            name="ops-layer-health-check", tags=["ecommerce", "ops"],
            description="Row counts per layer and the number of duplicate versions (should be 0). Read-only."),
        delta_inspect_flow.to_deployment(
            name="ops-delta-time-travel-inspect", tags=["ecommerce", "ops"],
            description="Read-only Delta history, time travel (as-of) and Change Data Feed (changes)."),
        lake_maintenance_flow.to_deployment(
            name="ops-weekly-lake-maintenance", tags=["ecommerce", "ops", "scheduled"],
            schedule=Cron(os.getenv("MAINTENANCE_CRON", "0 3 * * 0"), timezone=tz),
            description="Weekly: OPTIMIZE Delta tables with many small files; VACUUM dry run unless vacuum=true."),
        simulate_source_changes.to_deployment(
            name="demo-simulate-source-changes", tags=["ecommerce", "demo"],
            description="Demo: simulate a day of source changes (deliveries, customer moves, new orders)."),
        bronze_loss.to_deployment(
            name="demo-simulate-bronze-data-loss", tags=["ecommerce", "demo"],
            description="Demo: delete a date range from Bronze to practise recovery with the backfill."),
        skew_join_demo.to_deployment(
            name="demo-data-skew-joins", tags=["ecommerce", "demo"],
            description="Demo: timing of naive, broadcast, salted and AQE joins on skewed data."),
    ]
    serve(*deployments, pause_on_shutdown=False, limit=1)
