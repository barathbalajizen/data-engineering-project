"""Unit tests for Phase 3 Bronze → Silver transformation."""

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.transform.bronze_to_silver import (
    standardize_orders,
    standardize_reviews,
    standardize_products,
    deduplicate_orders,
    join_reviews_to_orders,
)
from src.quality.checks import (
    validate_silver_orders,
    validate_silver_reviews,
    validate_silver_products,
    validate_silver_orders_reviews,
)


@pytest.fixture
def sample_bronze_orders():
    """Create sample Bronze orders data."""
    return pl.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003"],
        "customer_id": ["CUST001", "CUST002", "CUST001"],
        "order_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "total_price": [100.50, 250.75, 45.25],
        "state": ["sp", "rj", "mg"],
        "_ingestion_timestamp": [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 2, 10, 0, 0),
            datetime(2024, 1, 3, 10, 0, 0),
        ],
        "_ingestion_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        "_source": ["orders_csv", "orders_csv", "orders_csv"],
    })


@pytest.fixture
def sample_bronze_reviews():
    """Create sample Bronze reviews data with sentiment."""
    return pl.DataFrame({
        "review_id": ["REV001", "REV002", "REV003"],
        "order_id": ["ORD001", "ORD002", "ORD001"],
        "product_id": ["PROD001", "PROD002", "PROD003"],
        "review_comment": ["Great!", "Broken and late", "Not bad"],
        "review_creation_date": ["2024-01-05", "2024-01-06", "2024-01-07"],
        "sentiment_score": [0.8, -0.6, 0.2],
        "sentiment_label": ["positive", "negative", "neutral"],
        "flagged_keywords": ["", "broken,late", ""],
        "has_flagged_keywords": [False, True, False],
        "_ingestion_date": [date(2024, 1, 5), date(2024, 1, 6), date(2024, 1, 7)],
        "_source": ["reviews_csv", "reviews_csv", "reviews_csv"],
    })


@pytest.fixture
def sample_bronze_products():
    """Create sample Bronze products data."""
    return pl.DataFrame({
        "product_id": ["PROD001", "PROD002", "PROD003"],
        "product_name": ["Widget A", "Widget B", "Gadget C"],
        "product_price": [50.00, 75.00, 100.00],
        "category": ["electronics", "electronics", "home"],
        "stock_quantity": [10, 20, 5],
        "_ingestion_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
        "_source": ["product_api", "product_api", "product_api"],
    })


def test_standardize_orders(sample_bronze_orders):
    """Test orders standardization."""
    result = standardize_orders(sample_bronze_orders)

    assert result.schema["order_date"] == pl.Datetime
    assert result.schema["total_price"] == pl.Float64
    assert result.get_column("state")[0] == "SP"  # uppercase
    assert result.get_column("state")[1] == "RJ"
    assert result.filter(pl.col("total_price") < 0).height == 0


def test_standardize_reviews(sample_bronze_reviews):
    """Test reviews standardization."""
    result = standardize_reviews(sample_bronze_reviews)

    assert "sentiment_score" in result.columns
    assert "sentiment_label" in result.columns
    assert "flagged_keywords" in result.columns
    assert "has_flagged_keywords" in result.columns
    labels = set(result.get_column("sentiment_label").to_list())
    assert labels.issubset({"positive", "neutral", "negative"})


def test_standardize_products(sample_bronze_products):
    """Test products standardization."""
    result = standardize_products(sample_bronze_products)

    assert result.schema["product_price"] == pl.Float64
    assert result.filter(pl.col("product_price") < 0).height == 0
    assert result.schema["product_id"] == pl.Utf8


def test_deduplicate_orders(sample_bronze_orders):
    """Test order deduplication keeps most recent."""
    # Add duplicate order with earlier timestamp
    duplicate = sample_bronze_orders.slice(0, 1).with_columns(
        pl.lit(datetime(2024, 1, 1, 8, 0, 0)).alias("_ingestion_timestamp")
    )
    duplicate_df = pl.concat([sample_bronze_orders, duplicate], how="vertical")

    result = deduplicate_orders(duplicate_df)

    assert len(result) == 3  # only 3 unique orders
    kept_row = result.filter(pl.col("order_id") == "ORD001")
    assert kept_row.get_column("_ingestion_timestamp")[0] == datetime(2024, 1, 1, 10, 0, 0)


def test_join_reviews_to_orders(sample_bronze_orders, sample_bronze_reviews):
    """Test joining reviews to orders."""
    result = join_reviews_to_orders(sample_bronze_orders, sample_bronze_reviews)

    assert len(result) >= len(sample_bronze_orders)  # left join
    assert "order_id" in result.columns
    assert "review_id" in result.columns
    # ORD001 has 2 reviews, ORD002 has 1, ORD003 has 0
    assert result.filter(pl.col("order_id") == "ORD001").height == 2
    assert result.filter(pl.col("order_id") == "ORD002").height == 1
    assert result.filter(pl.col("order_id") == "ORD003").height == 1


def test_validate_silver_orders(sample_bronze_orders):
    """Test Silver orders validation with pandera."""
    result = standardize_orders(sample_bronze_orders)

    # Should not raise
    validated = validate_silver_orders(result)
    assert len(validated) == len(result)


def test_validate_silver_reviews(sample_bronze_reviews):
    """Test Silver reviews validation with pandera."""
    result = standardize_reviews(sample_bronze_reviews)

    # Should not raise
    validated = validate_silver_reviews(result)
    assert len(validated) == len(result)


def test_validate_silver_products(sample_bronze_products):
    """Test Silver products validation with pandera."""
    result = standardize_products(sample_bronze_products)

    # Should not raise
    validated = validate_silver_products(result)
    assert len(validated) == len(result)


def test_validate_silver_orders_reviews(sample_bronze_orders, sample_bronze_reviews):
    """Test Silver orders+reviews joined validation."""
    orders = standardize_orders(sample_bronze_orders)
    reviews = standardize_reviews(sample_bronze_reviews)
    result = join_reviews_to_orders(orders, reviews)

    # Should not raise
    validated = validate_silver_orders_reviews(result)
    assert len(validated) == len(result)


def test_validate_silver_fails_on_negative_price():
    """Test that validation fails on invalid data."""
    invalid_df = pl.DataFrame({
        "order_id": ["ORD001"],
        "customer_id": ["CUST001"],
        "order_date": [datetime(2024, 1, 1)],
        "total_price": [-100.0],  # negative price
        "state": ["SP"],
        "_ingestion_date": [date(2024, 1, 1)],
        "_source": ["test"],
    })

    with pytest.raises(Exception):  # pandera SchemaError
        validate_silver_orders(invalid_df)


def test_validate_silver_fails_on_invalid_sentiment():
    """Test that validation fails on invalid sentiment label."""
    invalid_df = pl.DataFrame({
        "review_id": ["REV001"],
        "order_id": ["ORD001"],
        "product_id": ["PROD001"],
        "review_comment": ["test"],
        "review_creation_date": [datetime(2024, 1, 1)],
        "sentiment_score": [0.5],
        "sentiment_label": ["INVALID"],  # not one of the allowed values
        "flagged_keywords": [""],
        "has_flagged_keywords": [False],
        "_ingestion_date": [date(2024, 1, 1)],
        "_source": ["test"],
    })

    with pytest.raises(Exception):  # pandera SchemaError
        validate_silver_reviews(invalid_df)
