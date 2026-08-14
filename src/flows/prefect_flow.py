"""
Prefect Flow Deployment: E-Commerce Data Pipeline

Wraps the orchestration pipeline with Prefect for:
- Scheduling (daily runs)
- Retries on failure
- Error notifications
- Backfill capability
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Allow direct execution (`python src/flows/prefect_flow.py`) by making project root importable.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from prefect import flow, task
    PREFECT_AVAILABLE = True
except ImportError as import_error:
    PREFECT_AVAILABLE = False
    print(f"Prefect not available: {import_error}")
    print("Install with: pip install prefect")

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _parse_iso_date(value: str):
    """Parse ISO date string (YYYY-MM-DD) into date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_backfill_dates(run_date=None, start_date=None, end_date=None):
    """Resolve requested run dates for single-run or backfill execution."""
    if run_date:
        return [str(_parse_iso_date(run_date))]

    if (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("Provide both start_date and end_date for backfill runs")

    if start_date and end_date:
        start = _parse_iso_date(start_date)
        end = _parse_iso_date(end_date)
        if end < start:
            raise ValueError("end_date must be on or after start_date")

        number_of_days = (end - start).days + 1
        return [str(start + timedelta(days=offset)) for offset in range(number_of_days)]

    return [datetime.utcnow().date().isoformat()]


@task(name="Phase 1 Ingestion", retries=2, retry_delay_seconds=60)
def phase_1_task(run_date):
    """Prefect task for Phase 1: Data Ingestion"""
    from src.flows.ecommerce_pipeline import run_phase_1_ingestion
    return run_phase_1_ingestion(run_date=run_date)


@task(name="Phase 2 Sentiment Extraction", retries=2, retry_delay_seconds=60)
def phase_2_task(run_date):
    """Prefect task for Phase 2: Sentiment Extraction"""
    from src.flows.ecommerce_pipeline import run_phase_2_sentiment_extraction
    return run_phase_2_sentiment_extraction(run_date=run_date)


@task(name="Phase 3 Bronze to Silver", retries=2, retry_delay_seconds=60)
def phase_3_task(run_date):
    """Prefect task for Phase 3: Bronze to Silver"""
    from src.flows.ecommerce_pipeline import run_phase_3_bronze_to_silver
    return run_phase_3_bronze_to_silver(run_date=run_date)


@task(name="Phase 4 Silver to Gold", retries=2, retry_delay_seconds=60)
def phase_4_task(run_date):
    """Prefect task for Phase 4: Silver to Gold"""
    from src.flows.ecommerce_pipeline import run_phase_4_silver_to_gold
    return run_phase_4_silver_to_gold(run_date=run_date)


@task(name="Pipeline Summary")
def summary_task(phase1_result, phase2_result, phase3_result, phase4_result):
    """Log pipeline execution summary"""
    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Phase 1 Ingestion: {'PASSED' if phase1_result else 'FAILED'}")
    logger.info(f"Phase 2 Sentiment Extraction: {'PASSED' if phase2_result else 'FAILED'}")
    logger.info(f"Phase 3 Bronze to Silver: {'PASSED' if phase3_result else 'FAILED'}")
    logger.info(f"Phase 4 Silver to Gold: {'PASSED' if phase4_result else 'FAILED'}")
    logger.info("=" * 70)
    return all([phase1_result, phase2_result, phase3_result, phase4_result])


@flow(
    name="E-Commerce Data Pipeline",
    description="Orchestrated ETL pipeline for e-commerce data (Phases 1-4)"
)
def ecommerce_pipeline_flow(run_date=None, start_date=None, end_date=None):
    """Main Prefect flow orchestrating all 4 pipeline phases with backfill support."""
    logger.info("Starting E-Commerce Data Pipeline Flow")

    try:
        run_dates = _build_backfill_dates(run_date=run_date, start_date=start_date, end_date=end_date)
        logger.info(f"Flow will execute for {len(run_dates)} date(s): {run_dates}")

        per_date_results = []
        for current_run_date in run_dates:
            logger.info("=" * 70)
            logger.info(f"Executing pipeline for run_date={current_run_date}")
            logger.info("=" * 70)

            phase_1_result = phase_1_task(current_run_date)
            phase_2_result = phase_2_task(current_run_date)
            phase_3_result = phase_3_task(current_run_date)
            phase_4_result = phase_4_task(current_run_date)

            pipeline_success = summary_task(phase_1_result, phase_2_result, phase_3_result, phase_4_result)
            per_date_results.append({
                'run_date': current_run_date,
                'status': 'success' if pipeline_success else 'failed',
                'phases': {
                    'phase_1': phase_1_result,
                    'phase_2': phase_2_result,
                    'phase_3': phase_3_result,
                    'phase_4': phase_4_result,
                }
            })

        pipeline_success = all(item['status'] == 'success' for item in per_date_results)

        if pipeline_success:
            logger.info("Pipeline flow completed successfully")
        else:
            logger.error("Pipeline flow completed with errors")

        return {
            'status': 'success' if pipeline_success else 'failed',
            'run_dates': run_dates,
            'results': per_date_results,
        }

    except Exception as flow_error:
        logger.error(f"Pipeline flow failed: {str(flow_error)}")
        raise


def get_flow_schedule():
    """Return schedule for daily pipeline execution at 2 AM"""
    if PREFECT_AVAILABLE:
        from prefect.client.schemas.schedules import CronSchedule
        return CronSchedule(cron="0 2 * * *")  # 2 AM daily
    else:
        logger.warning("Prefect not available for scheduling")
        return None


if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Run Prefect flow with optional backfill window")
    cli_parser.add_argument("--run-date", help="Single run date (YYYY-MM-DD)")
    cli_parser.add_argument("--start-date", help="Backfill start date (YYYY-MM-DD)")
    cli_parser.add_argument("--end-date", help="Backfill end date (YYYY-MM-DD)")
    cli_args = cli_parser.parse_args()

    if PREFECT_AVAILABLE:
        result = ecommerce_pipeline_flow(
            run_date=cli_args.run_date,
            start_date=cli_args.start_date,
            end_date=cli_args.end_date,
        )
        print(f"\nFlow result: {result}")
    else:
        print("Prefect is required to run this flow")
        print("Install with: pip install prefect")
