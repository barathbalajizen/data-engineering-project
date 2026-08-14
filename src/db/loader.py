"""
PostgreSQL Database Loader: Loads Gold tables with idempotent upserts.

Manages connection to PostgreSQL and loads dimensional model tables
using ON CONFLICT DO UPDATE for safe, re-runnable pipeline.
"""

import os
import psycopg2
from psycopg2 import sql
from datetime import datetime
import polars as pl
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class PostgreSQLLoader:
    """Loads Gold dimensional tables into PostgreSQL with idempotent upserts."""

    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        """Initialize PostgreSQL connection parameters from environment."""
        self.host = host or os.getenv('PG_HOST', 'localhost')
        self.port = port or int(os.getenv('PG_PORT', '5432'))
        self.database = database or os.getenv('PG_DATABASE', 'ecommerce_db')
        self.user = user or os.getenv('PG_USER')
        self.password = password or os.getenv('PG_PASSWORD')
        self.connection = None
        self.cursor = None

        if not self.user or not self.password:
            raise ValueError(
                "PostgreSQL credentials not found. "
                "Set PG_USER and PG_PASSWORD in .env file"
            )

    def __enter__(self):
        """Context manager entry: establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: cleanup connection."""
        if exc_type:
            logger.error(f"Exception occurred: {exc_val}, rolling back")
            if self.connection:
                self.connection.rollback()
        self.close()

    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            logger.info(f"Connecting to PostgreSQL: {self.host}:{self.port}/{self.database}")
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            logger.info("Connected to PostgreSQL")
        except psycopg2.Error as error:
            logger.error(f"Failed to connect to PostgreSQL: {str(error)}")
            raise

    def close(self):
        """Close cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("PostgreSQL connection closed")

    def load_product_dimension(self, product_dataframe):
        """Upsert product dimension table."""
        logger.info(f"Upserting {len(product_dataframe)} rows into dim_product")

        rows_inserted = 0
        rows_updated = 0

        dataframe = product_dataframe if isinstance(product_dataframe, pl.DataFrame) else pl.DataFrame(product_dataframe)

        for row in dataframe.iter_rows(named=True):
            upsert_query = sql.SQL("""
                INSERT INTO dim_product
                    (product_id, product_name, category, price, stock_quantity,
                     is_active, created_at, updated_at, _source)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    stock_quantity = EXCLUDED.stock_quantity,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at
            """)

            query_parameters = (
                row['product_id'], row['product_name'], row['category'],
                row['price'], row['stock_quantity'], row['is_active'],
                str(row['created_at']), str(row['updated_at']), row['_source']
            )

            try:
                self.cursor.execute(upsert_query, query_parameters)
                if self.cursor.rowcount > 0:
                    if self.cursor.statusmessage.startswith('INSERT'):
                        rows_inserted += 1
                    else:
                        rows_updated += 1
            except psycopg2.Error as error:
                logger.error(f"Failed to upsert product {row['product_id']}: {str(error)}")
                raise

        self.cursor.execute("SELECT COUNT(*) FROM dim_product")
        total_rows = self.cursor.fetchone()[0]

        logger.info(f"dim_product: inserted={rows_inserted}, updated={rows_updated}, total={total_rows}")
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'total_rows': total_rows
        }

    def load_customer_dimension(self, customer_dataframe):
        """Upsert customer dimension table."""
        logger.info(f"Upserting {len(customer_dataframe)} rows into dim_customer")

        rows_inserted = 0
        rows_updated = 0

        dataframe = customer_dataframe if isinstance(customer_dataframe, pl.DataFrame) else pl.DataFrame(customer_dataframe)

        for row in dataframe.iter_rows(named=True):
            upsert_query = sql.SQL("""
                INSERT INTO dim_customer
                    (customer_id, customer_state, customer_city, customer_zip,
                     is_active, created_at, updated_at, _source)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_state = EXCLUDED.customer_state,
                    customer_city = EXCLUDED.customer_city,
                    customer_zip = EXCLUDED.customer_zip,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at
            """)

            query_parameters = (
                row['customer_id'], row['customer_state'], row['customer_city'],
                row['customer_zip'], row['is_active'],
                str(row['created_at']), str(row['updated_at']), row['_source']
            )

            try:
                self.cursor.execute(upsert_query, query_parameters)
                if self.cursor.rowcount > 0:
                    if self.cursor.statusmessage.startswith('INSERT'):
                        rows_inserted += 1
                    else:
                        rows_updated += 1
            except psycopg2.Error as error:
                logger.error(f"Failed to upsert customer {row['customer_id']}: {str(error)}")
                raise

        self.cursor.execute("SELECT COUNT(*) FROM dim_customer")
        total_rows = self.cursor.fetchone()[0]

        logger.info(f"dim_customer: inserted={rows_inserted}, updated={rows_updated}, total={total_rows}")
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'total_rows': total_rows
        }

    def load_date_dimension(self, date_dataframe):
        """Upsert date dimension table."""
        logger.info(f"Upserting {len(date_dataframe)} rows into dim_date")

        rows_inserted = 0
        rows_updated = 0

        dataframe = date_dataframe if isinstance(date_dataframe, pl.DataFrame) else pl.DataFrame(date_dataframe)

        for row in dataframe.iter_rows(named=True):
            upsert_query = sql.SQL("""
                INSERT INTO dim_date
                    (dim_date_id, date, year, quarter, month, day, day_of_week,
                     is_weekend, is_holiday, created_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dim_date_id) DO UPDATE SET
                    is_holiday = EXCLUDED.is_holiday
            """)

            query_parameters = (
                row['dim_date_id'], row['date'], row['year'], row['quarter'],
                row['month'], row['day'], row['day_of_week'],
                row['is_weekend'], row['is_holiday'], str(row['created_at'])
            )

            try:
                self.cursor.execute(upsert_query, query_parameters)
                if self.cursor.rowcount > 0:
                    if self.cursor.statusmessage.startswith('INSERT'):
                        rows_inserted += 1
                    else:
                        rows_updated += 1
            except psycopg2.Error as error:
                logger.error(f"Failed to upsert date {row['dim_date_id']}: {str(error)}")
                raise

        self.cursor.execute("SELECT COUNT(*) FROM dim_date")
        total_rows = self.cursor.fetchone()[0]

        logger.info(f"dim_date: inserted={rows_inserted}, updated={rows_updated}, total={total_rows}")
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'total_rows': total_rows
        }

    def load_fact_orders_table(self, orders_dataframe):
        """Upsert fact orders table."""
        logger.info(f"Upserting {len(orders_dataframe)} rows into fact_orders")

        rows_inserted = 0
        rows_updated = 0

        dataframe = orders_dataframe if isinstance(orders_dataframe, pl.DataFrame) else pl.DataFrame(orders_dataframe)

        for row in dataframe.iter_rows(named=True):
            upsert_query = sql.SQL("""
                INSERT INTO fact_orders
                    (order_id, dim_product_id, dim_customer_id, dim_date_id,
                     order_quantity, order_value, shipping_cost, total_amount,
                     sentiment_score, sentiment_label, has_flagged_keywords,
                     flagged_keywords, order_date, created_at, updated_at,
                     _ingestion_timestamp, _source)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    dim_product_id = EXCLUDED.dim_product_id,
                    dim_customer_id = EXCLUDED.dim_customer_id,
                    dim_date_id = EXCLUDED.dim_date_id,
                    order_quantity = EXCLUDED.order_quantity,
                    order_value = EXCLUDED.order_value,
                    shipping_cost = EXCLUDED.shipping_cost,
                    total_amount = EXCLUDED.total_amount,
                    sentiment_score = EXCLUDED.sentiment_score,
                    sentiment_label = EXCLUDED.sentiment_label,
                    has_flagged_keywords = EXCLUDED.has_flagged_keywords,
                    flagged_keywords = EXCLUDED.flagged_keywords,
                    updated_at = EXCLUDED.updated_at
            """)

            query_parameters = (
                row['order_id'], row['dim_product_id'], row['dim_customer_id'],
                row['dim_date_id'], row['order_quantity'], row['order_value'],
                row['shipping_cost'], row['total_amount'],
                row['sentiment_score'], row['sentiment_label'],
                row['has_flagged_keywords'], row['flagged_keywords'],
                str(row['order_date']), str(row['created_at']), str(row['updated_at']),
                str(row['_ingestion_timestamp']), row['_source']
            )

            try:
                self.cursor.execute(upsert_query, query_parameters)
                if self.cursor.rowcount > 0:
                    if self.cursor.statusmessage.startswith('INSERT'):
                        rows_inserted += 1
                    else:
                        rows_updated += 1
            except psycopg2.Error as error:
                logger.error(f"Failed to upsert order {row['order_id']}: {str(error)}")
                raise

        self.cursor.execute("SELECT COUNT(*) FROM fact_orders")
        total_rows = self.cursor.fetchone()[0]

        logger.info(f"fact_orders: inserted={rows_inserted}, updated={rows_updated}, total={total_rows}")
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'total_rows': total_rows
        }

    def load_gold_tables(self, gold_tables_dictionary):
        """Load all Gold dimensional model tables to PostgreSQL."""
        logger.info("=" * 60)
        logger.info("Loading Gold Tables to PostgreSQL")
        logger.info("=" * 60)

        load_statistics = {}

        try:
            if 'dim_product' in gold_tables_dictionary:
                load_statistics['dim_product'] = self.load_product_dimension(
                    gold_tables_dictionary['dim_product']
                )

            if 'dim_customer' in gold_tables_dictionary:
                load_statistics['dim_customer'] = self.load_customer_dimension(
                    gold_tables_dictionary['dim_customer']
                )

            if 'dim_date' in gold_tables_dictionary:
                load_statistics['dim_date'] = self.load_date_dimension(
                    gold_tables_dictionary['dim_date']
                )

            if 'fact_orders' in gold_tables_dictionary:
                load_statistics['fact_orders'] = self.load_fact_orders_table(
                    gold_tables_dictionary['fact_orders']
                )

            self.connection.commit()

            logger.info("\n" + "=" * 60)
            logger.info("Gold Tables Load Complete")
            logger.info("=" * 60)
            for table_name, table_stats in load_statistics.items():
                logger.info(f"{table_name}: {table_stats}")
            logger.info("=" * 60)

            return load_statistics

        except Exception as error:
            logger.error(f"\nGold load failed: {str(error)}")
            raise


if __name__ == '__main__':
    try:
        with PostgreSQLLoader() as loader:
            logger.info("PostgreSQL connection successful")
    except Exception as error:
        logger.error(f"PostgreSQL connection failed: {str(error)}")
