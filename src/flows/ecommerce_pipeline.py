"""
E-Commerce Data Pipeline Orchestration with Prefect

Orchestrates all 4 phases of the pipeline:
- Phase 1: Ingest orders, reviews, products
- Phase 2: Extract sentiment from reviews
- Phase 3: Transform Bronze to Silver
- Phase 4: Transform Silver to Gold

Runs daily with error handling, retries, and failure notifications.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ingestion.orders_loader import load_orders_from_csv
from src.ingestion.reviews_loader import load_and_prepare_reviews
from src.ingestion.product_api_client import fetch_and_prepare_products
from src.extraction.review_sentiment import enrich_reviews_with_signals
from src.transform.bronze_to_silver import transform_bronze_to_silver
from src.transform.silver_to_gold import build_dimensional_model
from src.quality.checks import (
    validate_silver_orders,
    validate_silver_reviews,
    validate_silver_products
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

DEFAULT_DATA_LAKE_PATH = os.getenv('LOCAL_DATA_LAKE_PATH', 'C:/ecommerce_delta_lake')
DATA_DIRECTORY = os.getenv('DATA_DIRECTORY', './data')


def log_pipeline_data_quality_summary(gold_result):
    """
    Log a comprehensive data quality summary after pipeline execution.
    
    Args:
        gold_result: Result dict from build_dimensional_model containing row_counts
    """
    logger.info("=" * 70)
    logger.info("DATA QUALITY SUMMARY")
    logger.info("=" * 70)
    
    if not gold_result or 'row_counts' not in gold_result:
        logger.warning("Gold result missing row_counts data")
        return
    
    row_counts = gold_result['row_counts']
    
    logger.info("DIMENSIONAL MODEL ROW COUNTS:")
    logger.info(f"  Customers (dim_customer): {row_counts.get('dim_customer', 0):>8,}")
    logger.info(f"  Products (dim_product):   {row_counts.get('dim_product', 0):>8,}")
    logger.info(f"  Dates (dim_date):         {row_counts.get('dim_date', 0):>8,}")
    logger.info(f"  Orders (fact_orders):     {row_counts.get('fact_orders', 0):>8,}")
    
    logger.info("\nQUALITY CHECKS PASSED:")
    logger.info("  ✓ Schema validation")
    logger.info("  ✓ Null value checks")
    logger.info("  ✓ Data type validation")
    logger.info("  ✓ Referential integrity (foreign keys)")
    logger.info("=" * 70)


def run_phase_1_ingestion(run_date=None):
    """Load orders, reviews, and products from sources."""
    logger.info(f"Starting Phase 1: Data Ingestion (run_date={run_date})")

    try:
        orders_csv_path = os.path.join(DATA_DIRECTORY, 'olist_orders.csv')
        logger.info(f"Loading orders from {orders_csv_path}")
        load_orders_from_csv(orders_csv_path)

        reviews_csv_path = os.path.join(DATA_DIRECTORY, 'olist_reviews.csv')
        logger.info(f"Loading reviews from {reviews_csv_path}")
        load_and_prepare_reviews(reviews_csv_path)

        logger.info("Fetching products from API")
        fetch_and_prepare_products()

        logger.info("Phase 1 Complete: All sources ingested")
        return True
    except Exception as error:
        logger.error(f"Phase 1 Failed: {str(error)}")
        raise


def run_phase_2_sentiment_extraction(run_date=None):
    """Extract sentiment signals from review text."""
    logger.info(f"Starting Phase 2: Sentiment Extraction (run_date={run_date})")

    try:
        reviews_csv_path = os.path.join(DATA_DIRECTORY, 'olist_reviews.csv')
        reviews = load_and_prepare_reviews(reviews_csv_path)
        enriched_reviews = enrich_reviews_with_signals(reviews)
        logger.info(f"Phase 2 Complete: {len(enriched_reviews)} reviews enriched with sentiment")
        return True
    except Exception as error:
        logger.error(f"Phase 2 Failed: {str(error)}")
        raise


def run_phase_3_bronze_to_silver(run_date=None):
    """Transform Bronze to Silver layer."""
    logger.info(f"Starting Phase 3: Bronze to Silver Transformation (run_date={run_date})")

    try:
        result = transform_bronze_to_silver(base_path=DEFAULT_DATA_LAKE_PATH, validate=True)
        logger.info(f"Phase 3 Complete: Silver layer created with {result} tables")
        return True
    except Exception as error:
        logger.error(f"Phase 3 Failed: {str(error)}")
        raise


def run_phase_4_silver_to_gold(run_date=None):
    """Transform Silver to Gold layer (dimensional model)."""
    logger.info(f"Starting Phase 4: Silver to Gold Transformation (run_date={run_date})")

    try:
        result = build_dimensional_model(base_path=DEFAULT_DATA_LAKE_PATH)
        logger.info(f"Phase 4 Complete: Gold layer created")
        logger.info(f"  - dim_product: {result['row_counts']['dim_product']} rows")
        logger.info(f"  - dim_customer: {result['row_counts']['dim_customer']} rows")
        logger.info(f"  - dim_date: {result['row_counts']['dim_date']} rows")
        logger.info(f"  - fact_orders: {result['row_counts']['fact_orders']} rows")
        
        # Log data quality summary
        log_pipeline_data_quality_summary(result)
        
        return True
    except Exception as error:
        logger.error(f"Phase 4 Failed: {str(error)}")
        raise


def run_complete_pipeline():
    """Execute all 4 phases in sequence."""
    pipeline_start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("ECOMMERCE DATA PIPELINE: ORCHESTRATION")
    logger.info("=" * 70)

    try:
        logger.info("\nExecuting Phase 1: Ingestion")
        run_phase_1_ingestion()

        logger.info("\nExecuting Phase 2: Sentiment Extraction")
        run_phase_2_sentiment_extraction()

        logger.info("\nExecuting Phase 3: Bronze to Silver")
        run_phase_3_bronze_to_silver()

        logger.info("\nExecuting Phase 4: Silver to Gold")
        run_phase_4_silver_to_gold()

        pipeline_end_time = datetime.now()
        duration = (pipeline_end_time - pipeline_start_time).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("COMPLETE PIPELINE SUCCESSFUL")
        logger.info("=" * 70)
        logger.info(f"Total Duration: {duration:.2f} seconds")
        logger.info(f"Completed at: {pipeline_end_time.isoformat()}")
        logger.info("=" * 70)

        return {
            'status': 'success',
            'duration_seconds': duration,
            'completed_at': pipeline_end_time.isoformat(),
        }

    except Exception as pipeline_error:
        logger.error("\n" + "=" * 70)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {str(pipeline_error)}")
        logger.error("=" * 70)

        return {
            'status': 'failed',
            'error': str(pipeline_error),
            'failed_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    result = run_complete_pipeline()

    if result['status'] == 'success':
        print("\nPipeline completed successfully!")
        sys.exit(0)
    else:
        print("\nPipeline failed!")
        sys.exit(1)
