"""
Dimensional Model Builder: Silver to Gold Transformation

Transforms clean Silver layer data into a star schema (fact + dimensions)
using Polars for efficient processing. Output is ready for PostgreSQL loading.
"""

import polars as pl
from datetime import datetime, timedelta
from deltalake import DeltaTable, write_deltalake
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def read_silver_table(base_path, table_source):
    """Read a Silver Delta table into a Polars DataFrame."""
    try:
        table_path = f"{base_path}/silver/{table_source}/"
        logger.info(f"Reading Silver table: {table_source} from {table_path}")

        delta_table = DeltaTable(table_path)
        dataframe = pl.from_arrow(delta_table.to_pyarrow_table())

        logger.info(f"Read {len(dataframe)} rows from Silver {table_source}")
        return dataframe
    except Exception as error:
        logger.error(f"Failed to read Silver {table_source}: {str(error)}")
        raise


def build_product_dimension(silver_products):
    """Build product dimension table with unique products and surrogate keys."""
    logger.info(f"Building product dimension from {len(silver_products)} Silver records")

    price_column = 'product_price' if 'product_price' in silver_products.columns else 'price'

    selected_columns = ['product_id', 'product_name', 'category', price_column]
    if 'stock_quantity' in silver_products.columns:
        selected_columns.append('stock_quantity')
    else:
        silver_products = silver_products.with_columns(pl.lit(0).alias('stock_quantity'))
        selected_columns.append('stock_quantity')

    unique_products = silver_products.select(selected_columns).unique(
        subset=['product_id'], keep='first'
    ).rename({price_column: 'price'})

    products_with_metadata = unique_products.with_columns([
        pl.lit(True).alias('is_active'),
        pl.lit(datetime.now()).alias('created_at'),
        pl.lit(datetime.now()).alias('updated_at'),
        pl.lit('silver_products').alias('_source'),
    ])

    products_with_surrogate_key = products_with_metadata.with_row_count(
        'dim_product_id', offset=1
    )

    result = products_with_surrogate_key.select([
        'dim_product_id', 'product_id', 'product_name', 'category', 'price',
        'stock_quantity', 'is_active', 'created_at', 'updated_at', '_source'
    ])

    logger.info(f"Built product dimension with {len(result)} unique products")
    return result


def build_customer_dimension(silver_orders):
    """Build customer dimension table with unique customers and surrogate keys."""
    logger.info(f"Building customer dimension from {len(silver_orders)} Silver orders")

    customer_cols = ['customer_id']
    for col in ['customer_state', 'customer_city', 'customer_zip']:
        if col in silver_orders.columns:
            customer_cols.append(col)

    unique_customers = silver_orders.select(customer_cols).unique(subset=['customer_id'], keep='first')

    if 'customer_state' not in unique_customers.columns:
        unique_customers = unique_customers.with_columns([
            pl.lit('Unknown').alias('customer_state'),
            pl.lit('Unknown').alias('customer_city'),
            pl.lit('Unknown').alias('customer_zip'),
        ])

    customers_with_metadata = unique_customers.with_columns([
        pl.lit(True).alias('is_active'),
        pl.lit(datetime.now()).alias('created_at'),
        pl.lit(datetime.now()).alias('updated_at'),
        pl.lit('silver_orders').alias('_source'),
    ])

    customers_with_surrogate_key = customers_with_metadata.with_row_count(
        'dim_customer_id', offset=1
    )

    result = customers_with_surrogate_key.select([
        'dim_customer_id', 'customer_id', 'customer_state', 'customer_city',
        'customer_zip', 'is_active', 'created_at', 'updated_at', '_source'
    ])

    logger.info(f"Built customer dimension with {len(result)} unique customers")
    return result


def build_date_dimension(start_date, end_date):
    """Build calendar date dimension with year, month, quarter, and weekend flags."""
    logger.info(f"Building date dimension from {start_date.date()} to {end_date.date()}")

    date_range = pl.datetime_range(start_date, end_date, "1d", eager=True)

    dates_with_attributes = pl.DataFrame({
        'date': date_range,
        'year': date_range.dt.year(),
        'quarter': date_range.dt.quarter(),
        'month': date_range.dt.month(),
        'day': date_range.dt.day(),
        'day_of_week': date_range.dt.weekday(),
        'is_weekend': date_range.dt.weekday() >= 5,
        'is_holiday': False,
        'created_at': datetime.now(),
    })

    dates_with_yyyymmdd_id = dates_with_attributes.with_columns(
        pl.col('date').dt.strftime('%Y%m%d').cast(pl.Int32).alias('dim_date_id')
    )

    result = dates_with_yyyymmdd_id.select([
        'dim_date_id', 'date', 'year', 'quarter', 'month', 'day',
        'day_of_week', 'is_weekend', 'is_holiday', 'created_at'
    ])

    logger.info(f"Built date dimension with {len(result)} days")
    return result


def build_orders_fact_table(
    silver_orders,
    silver_orders_with_reviews,
    product_dimension,
    customer_dimension,
    date_dimension
):
    """Build fact table joining orders to dimensions with sentiment and quality data."""
    logger.info(f"Building fact table from {len(silver_orders)} Silver orders")

    orders_base_columns = [
        pl.col('order_id'),
        pl.col('customer_id'),
        pl.col('order_date'),
        pl.col('order_quantity') if 'order_quantity' in silver_orders.columns else pl.lit(1).alias('order_quantity'),
        pl.col('order_value') if 'order_value' in silver_orders.columns else (
            pl.col('total_price') if 'total_price' in silver_orders.columns else pl.lit(0.0)
        ).alias('order_value'),
        pl.col('shipping_cost') if 'shipping_cost' in silver_orders.columns else pl.lit(0.0).alias('shipping_cost'),
        pl.col('total_amount') if 'total_amount' in silver_orders.columns else (
            pl.col('total_price') if 'total_price' in silver_orders.columns else pl.lit(0.0)
        ).alias('total_amount'),
        pl.col('_ingestion_timestamp') if '_ingestion_timestamp' in silver_orders.columns else pl.lit(datetime.now()).alias('_ingestion_timestamp'),
    ]

    orders_base = silver_orders.select(orders_base_columns)

    if len(silver_orders_with_reviews) > 0:
        available_review_columns = ['order_id']
        for column_name in [
            'product_id', 'product_id_order', 'product_id_review',
            'sentiment_score', 'sentiment_label', 'flagged_keywords',
            'has_flagged_keywords'
        ]:
            if column_name in silver_orders_with_reviews.columns:
                available_review_columns.append(column_name)

        review_signals = silver_orders_with_reviews.select(available_review_columns).unique(
            subset=['order_id'], keep='first'
        )
        orders_with_sentiment = orders_base.join(review_signals, on='order_id', how='left')
    else:
        orders_with_sentiment = orders_base.with_columns([
            pl.lit(None).alias('sentiment_score'),
            pl.lit(None).alias('sentiment_label'),
            pl.lit(None).alias('flagged_keywords'),
            pl.lit(False).alias('has_flagged_keywords'),
        ])

    product_id_candidates = [
        column_name for column_name in ['product_id', 'product_id_order', 'product_id_review']
        if column_name in orders_with_sentiment.columns
    ]
    if product_id_candidates:
        orders_with_sentiment = orders_with_sentiment.with_columns(
            pl.coalesce([pl.col(column_name) for column_name in product_id_candidates]).alias('product_id')
        )

    if 'product_id' in orders_with_sentiment.columns:
        orders_with_sentiment = orders_with_sentiment.with_columns(
            pl.col('product_id').cast(pl.Utf8, strict=False)
        )
    else:
        orders_with_sentiment = orders_with_sentiment.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('product_id')
        )

    cleanup_columns = [
        column_name for column_name in ['product_id_order', 'product_id_review']
        if column_name in orders_with_sentiment.columns
    ]
    if cleanup_columns:
        orders_with_sentiment = orders_with_sentiment.drop(cleanup_columns)

    for column_name, default_expr in [
        ('sentiment_score', pl.lit(None)),
        ('sentiment_label', pl.lit(None)),
        ('flagged_keywords', pl.lit(None)),
        ('has_flagged_keywords', pl.lit(False)),
    ]:
        if column_name not in orders_with_sentiment.columns:
            orders_with_sentiment = orders_with_sentiment.with_columns(default_expr.alias(column_name))

    product_keys = product_dimension.select([
        pl.col('product_id').cast(pl.Utf8, strict=False).alias('product_id'),
        'dim_product_id'
    ])
    orders_with_product_key = orders_with_sentiment.join(
        product_keys, on='product_id', how='left'
    )

    customer_keys = customer_dimension.select(['customer_id', 'dim_customer_id'])
    orders_with_customer_key = orders_with_product_key.join(
        customer_keys, on='customer_id', how='left'
    )

    orders_with_date_key = orders_with_customer_key.with_columns(
        pl.col('order_date').cast(pl.Date, strict=False).dt.strftime('%Y%m%d').cast(pl.Int32).alias('dim_date_id')
    )

    fact_table = orders_with_date_key.select([
        'order_id', 'dim_product_id', 'dim_customer_id', 'dim_date_id',
        'order_quantity', 'order_value', 'shipping_cost', 'total_amount',
        'sentiment_score', 'sentiment_label', 'has_flagged_keywords',
        'flagged_keywords', 'order_date', '_ingestion_timestamp'
    ]).with_columns([
        pl.lit(datetime.now()).alias('created_at'),
        pl.lit(datetime.now()).alias('updated_at'),
        pl.lit('silver_orders').alias('_source'),
    ])

    logger.info(f"Built fact table with {len(fact_table)} records")
    return fact_table


def write_gold_delta_table(dataframe, table_name, base_path):
    """Write a table to Gold layer Delta Lake storage."""
    try:
        table_path = f"{base_path}/gold/{table_name}/"
        logger.info(f"Writing Gold {table_name} to {table_path}")

        import shutil
        try:
            shutil.rmtree(table_path)
            logger.info(f"Cleared existing Gold {table_name}")
        except:
            pass

        write_deltalake(table_path, dataframe.to_arrow(), mode='append', engine='rust')
        logger.info(f"Wrote {len(dataframe)} rows to Gold {table_name}")
        return table_path
    except Exception as error:
        logger.error(f"Failed to write Gold {table_name}: {str(error)}")
        raise


def build_dimensional_model(base_path, validate=True):
    """
    Build complete star schema from Silver tables.

    Orchestrates transformation of Silver tables into Gold dimensional model:
    1. Reads Silver tables (orders, reviews, products)
    2. Builds dimension tables (product, customer, date)
    3. Builds fact table with foreign keys to dimensions
    4. Writes all tables to Gold Delta layer
    """
    logger.info("=" * 60)
    logger.info("Building Dimensional Model: Silver -> Gold (Polars)")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        logger.info("\n[Step 1/5] Reading Silver tables...")
        silver_orders = read_silver_table(base_path, 'orders')
        silver_products = read_silver_table(base_path, 'products')
        silver_reviews = read_silver_table(base_path, 'reviews')
        silver_orders_with_reviews = read_silver_table(base_path, 'orders_reviews')

        logger.info("\n[Step 2/5] Building dimension tables...")

        order_dates = silver_orders.select('order_date').to_series()
        min_order_date = order_dates.min()
        max_order_date = order_dates.max() + timedelta(days=7)
        date_dimension = build_date_dimension(min_order_date, max_order_date)

        product_dimension = build_product_dimension(silver_products)
        customer_dimension = build_customer_dimension(silver_orders)

        logger.info("\n[Step 3/5] Building fact table...")
        orders_fact = build_orders_fact_table(
            silver_orders,
            silver_orders_with_reviews,
            product_dimension,
            customer_dimension,
            date_dimension
        )

        logger.info("\n[Step 4/5] Writing Gold tables to Delta...")
        product_dimension_path = write_gold_delta_table(
            product_dimension, 'dim_product', base_path
        )
        customer_dimension_path = write_gold_delta_table(
            customer_dimension, 'dim_customer', base_path
        )
        date_dimension_path = write_gold_delta_table(
            date_dimension, 'dim_date', base_path
        )
        fact_table_path = write_gold_delta_table(
            orders_fact, 'fact_orders', base_path
        )

        logger.info("\n[Step 5/5] Summarizing build...")
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()

        result = {
            'product_dimension_path': product_dimension_path,
            'customer_dimension_path': customer_dimension_path,
            'date_dimension_path': date_dimension_path,
            'fact_table_path': fact_table_path,
            'row_counts': {
                'dim_product': len(product_dimension),
                'dim_customer': len(customer_dimension),
                'dim_date': len(date_dimension),
                'fact_orders': len(orders_fact),
            },
            'timestamp': end_time.isoformat(),
            'duration_seconds': duration_seconds,
        }

        logger.info("\n" + "=" * 60)
        logger.info("Dimensional Model Build Complete")
        logger.info("=" * 60)
        logger.info(f"dim_product:  {result['row_counts']['dim_product']:>6} rows")
        logger.info(f"dim_customer: {result['row_counts']['dim_customer']:>6} rows")
        logger.info(f"dim_date:     {result['row_counts']['dim_date']:>6} rows")
        logger.info(f"fact_orders:  {result['row_counts']['fact_orders']:>6} rows")
        logger.info(f"Duration: {duration_seconds:.2f} seconds")
        logger.info("=" * 60)

        return result

    except Exception as error:
        logger.error(f"\nDimensional model build failed: {str(error)}")
        raise


if __name__ == '__main__':
    base_path = 'C:/ecommerce_delta_lake'
    result = build_dimensional_model(base_path)
    print(f"\nDimensional model complete: {result}")
