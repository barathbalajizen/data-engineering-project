"""Phase 4 Unit Tests: Silver to Gold Transformation (Polars)
=========================================================

Tests for the dimensional modeling transformation using Polars.
"""

import pytest
import polars as pl
from datetime import datetime, timedelta
from src.transform.silver_to_gold import (
    build_product_dimension,
    build_customer_dimension,
    build_date_dimension,
    build_orders_fact_table,
)


@pytest.fixture
def sample_silver_products():
    """Sample Silver products for product dimension building."""
    return pl.DataFrame({
        'product_id': ['P001', 'P002', 'P001', 'P003'],
        'product_name': ['Laptop', 'Mouse', 'Laptop', 'Monitor'],
        'category': ['Electronics', 'Accessories', 'Electronics', 'Electronics'],
        'price': [999.99, 29.99, 999.99, 299.99],
        'stock_quantity': [10, 50, 10, 25],
    })


@pytest.fixture
def sample_silver_orders():
    """Sample Silver orders for dim_customer and fact building."""
    return pl.DataFrame({
        'order_id': ['O001', 'O002', 'O003', 'O004', 'O005'],
        'product_id': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'customer_id': ['C001', 'C002', 'C001', 'C001', 'C002'],
        'customer_state': ['SP', 'RJ', 'SP', 'SP', 'RJ'],
        'customer_city': ['Sao Paulo', 'Rio', 'Sao Paulo', 'Sao Paulo', 'Rio'],
        'customer_zip': ['01000', '20000', '01000', '01000', '20000'],
        'order_date': ['2024-01-10', '2024-01-11', '2024-01-12', '2024-01-13', '2024-01-14'],
        'order_quantity': [1, 2, 1, 1, 3],
        'order_value': [999.99, 59.98, 999.99, 299.99, 89.97],
        'shipping_cost': [50.0, 10.0, 50.0, 40.0, 15.0],
        'total_amount': [1049.99, 69.98, 1049.99, 339.99, 104.97],
        '_ingestion_timestamp': ['2024-01-10 10:00:00'] * 5,
    })


@pytest.fixture
def sample_silver_reviews():
    """Sample Silver reviews for sentiment data."""
    return pl.DataFrame({
        'review_id': ['R001', 'R002', 'R003', 'R004', 'R005'],
        'order_id': ['O001', 'O002', 'O003', 'O004', 'O005'],
        'sentiment_score': [0.8, 0.2, -0.7, 0.5, -0.3],
        'sentiment_label': ['positive', 'neutral', 'negative', 'positive', 'negative'],
        'flagged_keywords': [None, None, 'broken', None, 'late'],
        'has_flagged_keywords': [False, False, True, False, True],
    })


@pytest.fixture
def sample_silver_orders_reviews(sample_silver_orders, sample_silver_reviews):
    """Sample Silver orders_reviews (pre-joined table)."""
    return sample_silver_orders.join(sample_silver_reviews, on='order_id', how='left')


@pytest.fixture
def sample_dim_product(sample_silver_products):
    """Built dim_product from sample_silver_products."""
    return build_product_dimension(sample_silver_products)


@pytest.fixture
def sample_dim_customer(sample_silver_orders):
    """Built dim_customer from sample_silver_orders."""
    return build_customer_dimension(sample_silver_orders)


@pytest.fixture
def sample_dim_date():
    """Built dim_date for a 30-day range."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    return build_date_dimension(start, end)


# ============================================================================
# DIMENSION TABLE TESTS
# ============================================================================

def test_build_dim_product_deduplicates(sample_silver_products):
    """Test that build_product_dimension removes duplicates."""
    result = build_product_dimension(sample_silver_products)

    assert len(result) == 3
    assert len(result.select('product_id').unique()) == 3
    assert set(result['product_name'].to_list()) == {'Laptop', 'Mouse', 'Monitor'}


def test_build_dim_product_columns(sample_dim_product):
    """Test dim_product has all required columns."""
    required_cols = [
        'dim_product_id', 'product_id', 'product_name', 'category',
        'price', 'stock_quantity', 'is_active', 'created_at', 'updated_at', '_source'
    ]

    for col in required_cols:
        assert col in sample_dim_product.columns


def test_build_dim_product_surrogate_keys(sample_dim_product):
    """Test dim_product_id is sequential."""
    ids = sample_dim_product['dim_product_id'].to_list()
    assert ids == list(range(1, len(sample_dim_product) + 1))


def test_build_dim_customer_deduplicates(sample_silver_orders):
    """Test that build_dim_customer removes duplicate customers."""
    result = build_customer_dimension(sample_silver_orders)

    assert len(result) == 2
    assert set(result['customer_id'].to_list()) == {'C001', 'C002'}


def test_build_dim_customer_columns(sample_dim_customer):
    """Test dim_customer has all required columns."""
    required_cols = [
        'dim_customer_id', 'customer_id', 'customer_state', 'customer_city',
        'customer_zip', 'is_active', 'created_at', 'updated_at', '_source'
    ]

    for col in required_cols:
        assert col in sample_dim_customer.columns


def test_build_dim_date_range(sample_dim_date):
    """Test dim_date generates correct date range."""
    assert len(sample_dim_date) == 31


def test_build_dim_date_yyyymmdd_id(sample_dim_date):
    """Test dim_date_id is in YYYYMMDD format."""
    first_row = sample_dim_date.row(0, named=True)
    assert first_row['dim_date_id'] == 20240101

    last_row = sample_dim_date.row(sample_dim_date.height - 1, named=True)
    assert last_row['dim_date_id'] == 20240131


# ============================================================================
# FACT TABLE TESTS
# ============================================================================

def test_build_fact_orders_row_count(
    sample_silver_orders,
    sample_silver_orders_reviews,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_date
):
    """Test fact_orders has one row per order."""
    result = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_date
    )

    assert len(result) == 5


def test_build_fact_orders_columns(
    sample_silver_orders,
    sample_silver_orders_reviews,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_date
):
    """Test fact_orders has all required columns."""
    result = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_date
    )

    required_cols = [
        'order_id', 'dim_product_id', 'dim_customer_id', 'dim_date_id',
        'order_quantity', 'order_value', 'shipping_cost', 'total_amount',
        'sentiment_score', 'sentiment_label', 'has_flagged_keywords',
        'flagged_keywords', 'order_date', '_ingestion_timestamp',
        'created_at', 'updated_at', '_source'
    ]

    for col in required_cols:
        assert col in result.columns


def test_build_fact_orders_foreign_keys_not_null(
    sample_silver_orders,
    sample_silver_orders_reviews,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_date
):
    """Test all foreign keys in fact_orders are populated."""
    result = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_date
    )

    assert result['dim_product_id'].null_count() == 0
    assert result['dim_customer_id'].null_count() == 0
    assert result['dim_date_id'].null_count() == 0


def test_build_fact_orders_sentiment_joined(
    sample_silver_orders,
    sample_silver_orders_reviews,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_date
):
    """Test sentiment data is joined to fact_orders."""
    result = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_date
    )

    o003 = result.filter(pl.col('order_id') == 'O003').to_dicts()[0]
    assert o003['sentiment_score'] == -0.7
    assert o003['sentiment_label'] == 'negative'
    assert o003['has_flagged_keywords'] == True


def test_build_fact_orders_measures_preserved(
    sample_silver_orders,
    sample_silver_orders_reviews,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_date
):
    """Test order measures are preserved in fact table."""
    result = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_date
    )

    o001 = result.filter(pl.col('order_id') == 'O001').to_dicts()[0]
    assert o001['order_quantity'] == 1
    assert o001['order_value'] == 999.99
    assert o001['shipping_cost'] == 50.0
    assert o001['total_amount'] == 1049.99


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

def test_build_dim_product_empty_dataframe():
    """Test build_dim_product handles empty DataFrame."""
    empty_df = pl.DataFrame({
        'product_id': [],
        'product_name': [],
        'category': [],
        'price': [],
        'stock_quantity': [],
    })

    result = build_product_dimension(empty_df)
    assert len(result) == 0


def test_build_dim_customer_empty_dataframe():
    """Test build_dim_customer handles empty DataFrame."""
    empty_df = pl.DataFrame({
        'customer_id': [],
        'customer_state': [],
        'customer_city': [],
        'customer_zip': [],
    })

    result = build_customer_dimension(empty_df)
    assert len(result) == 0


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

def test_end_to_end_build(
    sample_silver_products,
    sample_silver_orders,
    sample_silver_reviews,
    sample_silver_orders_reviews
):
    """Test complete dimension and fact building workflow."""
    dim_product = build_product_dimension(sample_silver_products)
    dim_customer = build_customer_dimension(sample_silver_orders)
    dim_date = build_date_dimension(datetime(2024, 1, 1), datetime(2024, 1, 31))
    fact_orders = build_orders_fact_table(
        sample_silver_orders,
        sample_silver_orders_reviews,
        dim_product,
        dim_customer,
        dim_date
    )

    fact_products = set(fact_orders['dim_product_id'].to_list())
    dim_products = set(dim_product['dim_product_id'].to_list())
    assert fact_products.issubset(dim_products)

    fact_customers = set(fact_orders['dim_customer_id'].to_list())
    dim_customers = set(dim_customer['dim_customer_id'].to_list())
    assert fact_customers.issubset(dim_customers)

    fact_dates = set(fact_orders['dim_date_id'].to_list())
    dim_dates = set(dim_date['dim_date_id'].to_list())
    assert fact_dates.issubset(dim_dates)
