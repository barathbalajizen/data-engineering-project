"""
Gold Layer Pipeline: Orchestrates dimensional modeling and PostgreSQL load.

Reads clean Silver layer data, builds dimensional model using Polars,
writes Gold Delta tables, and loads to PostgreSQL with idempotent upserts.
"""

import sys
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.transform.silver_to_gold import build_dimensional_model
from src.db.loader import PostgreSQLLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

DEFAULT_DELTA_LAKE_PATH = 'C:/ecommerce_delta_lake'


def load_gold_tables_from_delta(base_path):
    """Read Gold Delta tables into Polars DataFrames."""
    logger.info("Reading Gold tables from Delta Lake...")

    gold_tables = {}
    table_names = ['dim_product', 'dim_customer', 'dim_date', 'fact_orders']

    for table_name in table_names:
        try:
            table_path = f"{base_path}/gold/{table_name}/"
            delta_table = DeltaTable(table_path)
            gold_tables[table_name] = pl.from_arrow(delta_table.to_pyarrow_table())
            logger.info(f"Read {len(gold_tables[table_name]):>6} rows from {table_name}")
        except Exception as error:
            logger.error(f"Failed to read {table_name}: {str(error)}")
            raise

    return gold_tables


def print_execution_summary(dimensional_model_result, database_load_stats):
    """Print formatted summary of pipeline execution."""
    print("\n" + "=" * 70)
    print("GOLD LAYER PIPELINE: DIMENSIONAL MODELING + POSTGRESQL LOAD")
    print("=" * 70)

    print("\nDIMENSIONAL MODEL BUILD (Delta Lake)")
    print("-" * 70)
    for table_name, row_count in dimensional_model_result['row_counts'].items():
        print(f"  {table_name:15} -> {row_count:>6} rows")

    print(f"\nBuild Duration: {dimensional_model_result['duration_seconds']:.2f} seconds")

    print("\nPOSTGRESQL LOAD SUMMARY")
    print("-" * 70)
    for table_name, load_stats in database_load_stats.items():
        print(f"  {table_name:15}")
        print(f"    ├─ Inserted: {load_stats['rows_inserted']:>6}")
        print(f"    ├─ Updated:  {load_stats['rows_updated']:>6}")
        print(f"    └─ Total:    {load_stats['total_rows']:>6}")

    print("\nGOLD LAYER PIPELINE COMPLETE")
    print("=" * 70)
    print("\nNext Steps:")
    print("  1. Run tests: pytest tests/unit/test_silver_to_gold.py -v")
    print("  2. Query data: psql -d ecommerce_db")
    print("  3. Start Phase 5: Orchestration with Prefect")
    print()


def execute_gold_layer_pipeline(base_path=DEFAULT_DELTA_LAKE_PATH, skip_postgresql=False):
    """
    Execute complete Gold layer pipeline.

    Steps:
    1. Build dimensional model from Silver tables
    2. Load Gold tables to PostgreSQL (optional)
    3. Print summary and next steps
    """
    pipeline_start_time = datetime.now()

    try:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 1: Build Dimensional Model")
        logger.info("=" * 70)
        dimensional_model_result = build_dimensional_model(base_path)

        database_load_stats = {}
        if not skip_postgresql:
            logger.info("\n" + "=" * 70)
            logger.info("STEP 2: Load Gold Tables to PostgreSQL")
            logger.info("=" * 70)

            gold_tables = load_gold_tables_from_delta(base_path)

            try:
                with PostgreSQLLoader() as database_loader:
                    database_load_stats = database_loader.load_gold_to_postgresql(gold_tables)
            except Exception as connection_error:
                logger.warning(
                    f"PostgreSQL load skipped (connection failed): {str(connection_error)}"
                )
                logger.info("Gold tables remain available in Delta Lake.")
                skip_postgresql = True

        pipeline_end_time = datetime.now()
        total_duration = (pipeline_end_time - pipeline_start_time).total_seconds()

        print_execution_summary(dimensional_model_result, database_load_stats)

        logger.info(f"Total Pipeline Duration: {total_duration:.2f} seconds")

        return {
            'dimensional_model': dimensional_model_result,
            'database_load': database_load_stats,
            'skipped_postgresql': skip_postgresql,
            'total_duration_seconds': total_duration,
        }

    except Exception as pipeline_error:
        logger.error(f"\nGold layer pipeline failed: {str(pipeline_error)}")
        raise


if __name__ == '__main__':
    skip_database_load = '--skip-postgresql' in sys.argv or '--delta-only' in sys.argv

    try:
        result = execute_gold_layer_pipeline(skip_postgresql=skip_database_load)
        sys.exit(0)
    except Exception as error:
        logger.error(f"\nGold layer pipeline execution failed: {str(error)}")
        sys.exit(1)
