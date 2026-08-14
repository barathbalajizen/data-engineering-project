"""Unit tests for orders loader module."""

import pytest
import polars as pl
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ingestion.orders_loader import (
    validate_orders_schema,
    standardize_orders_data,
    add_bronze_metadata
)


@pytest.fixture
def sample_orders_df():
    """Create sample orders DataFrame for testing."""
    return pl.DataFrame({
        'order_id': ['1001', '1002', '1003'],
        'customer_id': ['c001', 'c002', 'c003'],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'total_price': ['100.50', '250.75', '45.25'],
        'state': ['sp', 'rj', 'mg']
    })


def test_validate_orders_schema_valid(sample_orders_df):
    """Test schema validation with valid data."""
    assert validate_orders_schema(sample_orders_df) is True


def test_validate_orders_schema_missing_column():
    """Test schema validation fails with missing columns."""
    df = pl.DataFrame({'order_id': [1, 2], 'customer_id': [1, 2]})

    with pytest.raises(ValueError, match="Missing columns"):
        validate_orders_schema(df)


def test_standardize_orders_data(sample_orders_df):
    """Test data standardization."""
    df = standardize_orders_data(sample_orders_df)

    assert df.schema['order_date'] == pl.Datetime
    assert df.schema['total_price'] == pl.Float64
    assert df.get_column('state')[0] == 'SP'  # uppercase


def test_standardize_handles_invalid_dates():
    """Test standardization with invalid dates."""
    df = pl.DataFrame({
        'order_id': ['1001', '1002'],
        'customer_id': ['c001', 'c002'],
        'order_date': ['2024-01-01', 'invalid-date'],
        'total_price': ['100.50', '250.75'],
        'state': ['SP', 'RJ']
    })

    result = standardize_orders_data(df)
    assert result.get_column('order_date').null_count() == 1


def test_add_bronze_metadata(sample_orders_df):
    """Test metadata addition."""
    df = add_bronze_metadata(sample_orders_df)

    assert '_ingestion_timestamp' in df.columns
    assert '_source' in df.columns
    assert '_ingestion_date' in df.columns
    assert df.get_column('_source')[0] == 'orders'


def test_standardize_handles_null_prices():
    """Test handling of null prices."""
    df = pl.DataFrame({
        'order_id': ['1001', '1002'],
        'customer_id': ['c001', 'c002'],
        'order_date': ['2024-01-01', '2024-01-02'],
        'total_price': [100.50, None],
        'state': ['SP', 'RJ']
    })

    result = standardize_orders_data(df)
    assert result.get_column('total_price').null_count() == 1
