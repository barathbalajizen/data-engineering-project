"""Load order data from CSV files into Bronze layer."""

import polars as pl
from pathlib import Path
from datetime import datetime
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.logger import setup_logger
from src.storage.local_storage import DEFAULT_LOCAL_DATA_LAKE_PATH, write_local_bronze
from tenacity import retry, stop_after_attempt, wait_exponential

logger = setup_logger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def load_orders_from_csv(csv_path: str) -> pl.DataFrame:
    """
    Load order data from CSV with retry logic.

    Args:
        csv_path: Path to orders CSV file

    Returns:
        DataFrame with orders data

    Raises:
        FileNotFoundError: If CSV file not found
        Exception: If CSV parsing fails
    """
    logger.info(f"Loading orders from: {csv_path}")

    try:
        df = pl.read_csv(csv_path)
        logger.info(f"Successfully loaded {len(df)} orders from {csv_path}")
        return df

    except FileNotFoundError as e:
        logger.error(f"Orders CSV file not found: {csv_path}")
        raise

    except Exception as e:
        logger.error(f"Error loading orders CSV: {str(e)}")
        raise


def validate_orders_schema(df: pl.DataFrame) -> bool:
    """
    Validate that loaded orders have expected columns.

    Args:
        df: DataFrame to validate

    Returns:
        True if valid, raises exception otherwise
    """
    required_columns = {
        'order_id', 'customer_id', 'order_date',
        'total_price', 'state'
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

    logger.info("Orders schema validation passed")
    return True


def standardize_orders_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Standardize orders data types and formats.

    Args:
        df: Raw orders DataFrame

    Returns:
        Standardized DataFrame
    """
    df = df.with_columns([
        pl.col('order_date').cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, strict=False),
        pl.col('total_price').cast(pl.Float64, strict=False),
        pl.col('customer_id').cast(pl.Utf8, strict=False),
        pl.col('order_id').cast(pl.Utf8, strict=False),
        pl.col('state').cast(pl.Utf8, strict=False).str.to_uppercase(),
    ])

    logger.info(f"Standardized {len(df)} orders records")
    return df


def add_bronze_metadata(df: pl.DataFrame, source: str = "orders") -> pl.DataFrame:
    """
    Add Bronze layer metadata to records.

    Args:
        df: DataFrame
        source: Data source name

    Returns:
        DataFrame with metadata columns
    """
    return df.with_columns([
        pl.lit(datetime.utcnow()).alias('_ingestion_timestamp'),
        pl.lit(source).alias('_source'),
        pl.lit(datetime.utcnow().date()).alias('_ingestion_date'),
    ])


def load_and_prepare_orders(csv_path: str) -> pl.DataFrame:
    """
    End-to-end orders loading pipeline.

    Args:
        csv_path: Path to orders CSV

    Returns:
        Ready-to-load DataFrame with Bronze metadata
    """
    logger.info("Starting orders ingestion pipeline")

    # Load
    df = load_orders_from_csv(csv_path)

    # Validate
    validate_orders_schema(df)

    # Standardize
    df = standardize_orders_data(df)

    # Add metadata
    df = add_bronze_metadata(df, source="orders_csv")

    logger.info(f"Orders pipeline complete: {len(df)} records ready")
    return df


if __name__ == "__main__":
    # Example usage
    import os
    csv_path = os.getenv("ORDERS_CSV_PATH", "./data/olist_orders.csv")
    local_lake_path = os.getenv("LOCAL_DATA_LAKE_PATH", DEFAULT_LOCAL_DATA_LAKE_PATH)
    storage_format = os.getenv("LOCAL_STORAGE_FORMAT", "delta")

    try:
        orders_df = load_and_prepare_orders(csv_path)
        output_path = write_local_bronze(
            orders_df,
            source="orders",
            base_path=local_lake_path,
            file_format=storage_format,
        )
        print(f"\nLoaded {len(orders_df)} orders")
        print(f"Stored Bronze data locally: {output_path}")
        print(f"Columns: {list(orders_df.columns)}")
        print("\nFirst few rows:")
        print(orders_df.head())
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
