"""Load customer reviews from CSV files into Bronze layer."""

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
def load_reviews_from_csv(csv_path: str) -> pl.DataFrame:
    """
    Load review data from CSV with retry logic.

    Args:
        csv_path: Path to reviews CSV file

    Returns:
        DataFrame with reviews data

    Raises:
        FileNotFoundError: If CSV file not found
        Exception: If CSV parsing fails
    """
    logger.info(f"Loading reviews from: {csv_path}")

    try:
        df = pl.read_csv(csv_path)
        logger.info(f"Successfully loaded {len(df)} reviews from {csv_path}")
        return df

    except FileNotFoundError as e:
        logger.error(f"Reviews CSV file not found: {csv_path}")
        raise

    except Exception as e:
        logger.error(f"Error loading reviews CSV: {str(e)}")
        raise


def validate_reviews_schema(df: pl.DataFrame) -> bool:
    """
    Validate that loaded reviews have expected columns.

    Args:
        df: DataFrame to validate

    Returns:
        True if valid, raises exception otherwise
    """
    required_columns = {
        'review_id', 'order_id', 'product_id',
        'review_comment', 'review_creation_date'
    }

    # Check if we have at least order_id and review_comment
    has_order = 'order_id' in df.columns
    has_comment = 'review_comment' in df.columns or 'comment' in df.columns

    if not (has_order and has_comment):
        logger.error(f"Missing critical columns. Found: {set(df.columns)}")
        raise ValueError("Missing order_id or review_comment columns")

    logger.info("Reviews schema validation passed")
    return True


def standardize_reviews_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Standardize reviews data types and formats.

    Args:
        df: Raw reviews DataFrame

    Returns:
        Standardized DataFrame
    """
    # Standardize column names
    df = df.rename({column: column.lower().strip() for column in df.columns})

    # Handle review_comment column (might have different names)
    if 'comment' in df.columns and 'review_comment' not in df.columns:
        df = df.with_columns(pl.col('comment').alias('review_comment')).drop('comment')

    # Convert ID columns to string
    if 'review_id' in df.columns:
        df = df.with_columns(pl.col('review_id').cast(pl.Utf8, strict=False))

    df = df.with_columns(pl.col('order_id').cast(pl.Utf8, strict=False))

    if 'product_id' in df.columns:
        df = df.with_columns(pl.col('product_id').cast(pl.Utf8, strict=False))

    # Convert date column
    if 'review_creation_date' in df.columns:
        df = df.with_columns(
            pl.col('review_creation_date').cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, strict=False)
        )

    # Ensure review_comment is string
    df = df.with_columns(
        pl.col('review_comment').fill_null('').cast(pl.Utf8, strict=False)
    )

    # Remove empty reviews
    df = df.filter(pl.col('review_comment').str.len_chars() > 0)

    logger.info(f"Standardized {len(df)} review records")
    return df


def add_bronze_metadata(df: pl.DataFrame, source: str = "reviews") -> pl.DataFrame:
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


def load_and_prepare_reviews(csv_path: str) -> pl.DataFrame:
    """
    End-to-end reviews loading pipeline.

    Args:
        csv_path: Path to reviews CSV

    Returns:
        Ready-to-load DataFrame with Bronze metadata
    """
    logger.info("Starting reviews ingestion pipeline")

    # Load
    df = load_reviews_from_csv(csv_path)

    # Validate
    validate_reviews_schema(df)

    # Standardize
    df = standardize_reviews_data(df)

    # Add metadata
    df = add_bronze_metadata(df, source="reviews_csv")

    logger.info(f"Reviews pipeline complete: {len(df)} records ready")
    return df


if __name__ == "__main__":
    # Example usage
    import os
    csv_path = os.getenv("REVIEWS_CSV_PATH", "./data/olist_reviews.csv")
    local_lake_path = os.getenv("LOCAL_DATA_LAKE_PATH", DEFAULT_LOCAL_DATA_LAKE_PATH)
    storage_format = os.getenv("LOCAL_STORAGE_FORMAT", "delta")

    try:
        reviews_df = load_and_prepare_reviews(csv_path)
        output_path = write_local_bronze(
            reviews_df,
            source="reviews",
            base_path=local_lake_path,
            file_format=storage_format,
        )
        print(f"\nLoaded {len(reviews_df)} reviews")
        print(f"Stored Bronze data locally: {output_path}")
        print(f"Columns: {list(reviews_df.columns)}")
        print("\nFirst few rows:")
        print(reviews_df.head())
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
