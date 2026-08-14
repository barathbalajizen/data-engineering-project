"""Transform Bronze data to Silver layer with standardization, joining, and deduplication."""

from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from src.utils.logger import setup_logger
from src.quality.checks import (
    validate_silver_orders,
    validate_silver_reviews,
    validate_silver_products,
    validate_silver_orders_reviews,
)

logger = setup_logger(__name__)


def read_bronze_table(
    base_path: str,
    source: str,
) -> pl.DataFrame:
    """Read a Bronze Delta table."""
    table_path = Path(base_path) / "bronze" / source
    logger.info(f"Reading Bronze table: {table_path}")

    try:
        table = DeltaTable(str(table_path))
        df = pl.from_arrow(table.to_pyarrow_table())
        logger.info(f"Read {len(df)} records from Bronze {source}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Bronze {source}: {str(e)}")
        raise


def standardize_orders(df: pl.DataFrame) -> pl.DataFrame:
    """Standardize orders data types and formats."""
    result = df.with_columns([
        pl.col("order_id").cast(pl.Utf8, strict=False),
        pl.col("customer_id").cast(pl.Utf8, strict=False),
        pl.col("order_date").cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, strict=False),
        pl.col("total_price").cast(pl.Float64, strict=False),
        pl.col("state").cast(pl.Utf8, strict=False).str.to_uppercase(),
    ])

    if "_ingestion_date" not in result.columns:
        result = result.with_columns(pl.lit(datetime.utcnow().date()).alias("_ingestion_date"))
    else:
        result = result.with_columns(
            pl.col("_ingestion_date").cast(pl.Utf8, strict=False).str.strptime(pl.Date, strict=False)
        )

    logger.info(f"Standardized {len(result)} orders")
    return result


def standardize_reviews(df: pl.DataFrame) -> pl.DataFrame:
    """Standardize reviews data types and formats."""
    result = df.with_columns([
        pl.col("review_id").cast(pl.Utf8, strict=False),
        pl.col("order_id").cast(pl.Utf8, strict=False),
        pl.col("product_id").cast(pl.Utf8, strict=False),
        pl.col("review_creation_date").cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, strict=False),
    ])

    # Ensure sentiment columns exist
    if "sentiment_score" not in result.columns:
        result = result.with_columns(pl.lit(0.0).alias("sentiment_score"))
    if "sentiment_label" not in result.columns:
        result = result.with_columns(pl.lit("neutral").alias("sentiment_label"))
    if "flagged_keywords" not in result.columns:
        result = result.with_columns(pl.lit("").alias("flagged_keywords"))
    if "has_flagged_keywords" not in result.columns:
        result = result.with_columns(pl.lit(False).alias("has_flagged_keywords"))

    # Ensure metadata columns
    if "_ingestion_date" not in result.columns:
        result = result.with_columns(pl.lit(datetime.utcnow().date()).alias("_ingestion_date"))
    else:
        result = result.with_columns(
            pl.col("_ingestion_date").cast(pl.Utf8, strict=False).str.strptime(pl.Date, strict=False)
        )

    logger.info(f"Standardized {len(result)} reviews")
    return result


def standardize_products(df: pl.DataFrame) -> pl.DataFrame:
    """Standardize products data types and formats."""
    result = df.with_columns([
        pl.col("product_id").cast(pl.Utf8, strict=False),
        pl.col("product_name").cast(pl.Utf8, strict=False),
        pl.col("product_price").cast(pl.Float64, strict=False),
    ])

    if "stock_quantity" in result.columns:
        result = result.with_columns(pl.col("stock_quantity").cast(pl.Float64, strict=False))

    # Ensure metadata columns
    if "_ingestion_date" not in result.columns:
        result = result.with_columns(pl.lit(datetime.utcnow().date()).alias("_ingestion_date"))
    else:
        result = result.with_columns(
            pl.col("_ingestion_date").cast(pl.Utf8, strict=False).str.strptime(pl.Date, strict=False)
        )

    logger.info(f"Standardized {len(result)} products")
    return result


def deduplicate_orders(df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicate orders, keeping the most recent."""
    if df.is_empty():
        return df

    result = df.sort("_ingestion_timestamp", descending=True, nulls_last=True)
    result = result.unique(subset=["order_id"], keep="first")

    logger.info(f"After deduplication: {len(result)} unique orders")
    return result


def join_reviews_to_orders(
    orders_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """Join reviews to orders on order_id with left outer join."""
    logger.info(f"Joining {len(reviews_df)} reviews to {len(orders_df)} orders")

    result = orders_df.join(reviews_df, on="order_id", how="left", suffix="_review")

    logger.info(f"After join: {len(result)} order-review records")
    return result


def write_silver_table(
    df: pl.DataFrame,
    layer: str,
    source: str,
    base_path: str,
    schema_validator=None,
) -> Path:
    """Write a DataFrame to Silver layer with validation."""
    if df.is_empty():
        logger.warning(f"Skipping Silver write for empty DataFrame: {source}")
        return Path()

    # Validate schema if provided
    if schema_validator:
        try:
            schema_validator(df)
            logger.info(f"Schema validation passed for {source}")
        except Exception as e:
            logger.error(f"Schema validation failed for {source}: {str(e)}")
            raise

    output_path = Path(base_path) / layer / source
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Replace existing table to avoid stale schema (e.g., legacy pandas index columns).
    import shutil
    if output_path.exists():
        shutil.rmtree(output_path)

    write_deltalake(
        str(output_path),
        df.to_arrow(),
        mode="overwrite",
        partition_by=["_ingestion_date"] if "_ingestion_date" in df.columns else None,
    )

    logger.info(f"Wrote {len(df)} records to Silver {source}: {output_path}")
    return output_path


def transform_bronze_to_silver(
    base_path: str = "C:/ecommerce_delta_lake",
    validate: bool = True,
) -> dict:
    """
    End-to-end Bronze to Silver transformation.

    Args:
        base_path: Root path for data lake
        validate: Whether to validate with pandera schemas

    Returns:
        Dictionary with paths to written Silver tables
    """
    logger.info("Starting Bronze to Silver transformation")

    # Read Bronze tables
    bronze_orders = read_bronze_table(base_path, "orders")
    bronze_reviews = read_bronze_table(base_path, "reviews")
    bronze_products = read_bronze_table(base_path, "products")

    # Standardize
    silver_orders = standardize_orders(bronze_orders)
    silver_reviews = standardize_reviews(bronze_reviews)
    silver_products = standardize_products(bronze_products)

    # Deduplicate orders
    silver_orders = deduplicate_orders(silver_orders)

    # Validate individual tables if requested
    if validate:
        validate_silver_orders(silver_orders)
        validate_silver_reviews(silver_reviews)
        validate_silver_products(silver_products)

    # Write individual Silver tables
    orders_path = write_silver_table(
        silver_orders,
        "silver",
        "orders",
        base_path,
        validate_silver_orders if validate else None,
    )
    reviews_path = write_silver_table(
        silver_reviews,
        "silver",
        "reviews",
        base_path,
        validate_silver_reviews if validate else None,
    )
    products_path = write_silver_table(
        silver_products,
        "silver",
        "products",
        base_path,
        validate_silver_products if validate else None,
    )

    # Join reviews to orders
    silver_orders_reviews = join_reviews_to_orders(silver_orders, silver_reviews)

    # Validate joined table if requested
    if validate:
        validate_silver_orders_reviews(silver_orders_reviews)

    # Write joined Silver table
    orders_reviews_path = write_silver_table(
        silver_orders_reviews,
        "silver",
        "orders_reviews",
        base_path,
        validate_silver_orders_reviews if validate else None,
    )

    logger.info("Bronze to Silver transformation complete")

    return {
        "orders": str(orders_path),
        "reviews": str(reviews_path),
        "products": str(products_path),
        "orders_reviews": str(orders_reviews_path),
    }
