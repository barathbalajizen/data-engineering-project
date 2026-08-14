"""Fetch product catalog from REST API into Bronze layer."""

import requests
import polars as pl
from typing import List, Dict, Optional
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.logger import setup_logger
from src.storage.local_storage import DEFAULT_LOCAL_DATA_LAKE_PATH, write_local_bronze
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime

logger = setup_logger(__name__)


class ProductAPIClient:
    """Client for fetching product data from REST API."""

    def __init__(
        self,
        base_url: str = "https://dummyjson.com",
        timeout: int = 30,
        max_retries: int = 3
    ):
        """
        Initialize API client.

        Args:
            base_url: Base URL for API (default: DummyJSON)
            timeout: Request timeout in seconds
            max_retries: Number of retry attempts
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        logger.info(f"ProductAPIClient initialized: {base_url}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_products(self, limit: int = 100, skip: int = 0) -> Dict:
        """
        Fetch products from API with pagination and retry logic.

        Args:
            limit: Number of products per request
            skip: Number of products to skip (for pagination)

        Returns:
            API response as dictionary

        Raises:
            requests.RequestException: If API call fails
        """
        endpoint = f"{self.base_url}/products"
        params = {"limit": limit, "skip": skip}

        try:
            logger.info(f"Fetching products: limit={limit}, skip={skip}")
            response = self.session.get(
                endpoint,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.info(
                f"Successfully fetched {len(data.get('products', []))} products"
            )
            return data

        except requests.exceptions.Timeout:
            logger.error(f"API request timeout: {endpoint}")
            raise

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error from API: {str(e)}")
            raise

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    def fetch_all_products(self, batch_size: int = 100) -> List[Dict]:
        """
        Fetch all products by handling pagination.

        Args:
            batch_size: Number of products per request

        Returns:
            List of all product dictionaries
        """
        all_products = []
        skip = 0

        try:
            while True:
                response = self.fetch_products(limit=batch_size, skip=skip)
                products = response.get("products", [])

                if not products:
                    break

                all_products.extend(products)
                total = response.get("total", len(all_products))

                logger.info(
                    f"Progress: {len(all_products)}/{total} products fetched"
                )

                if len(all_products) >= total:
                    break

                skip += batch_size

        except Exception as e:
            logger.error(f"Error fetching all products: {str(e)}")
            if all_products:
                logger.warning(f"Returning {len(all_products)} products fetched so far")
                return all_products
            raise

        return all_products

    def close(self):
        """Close API session."""
        self.session.close()
        logger.info("API session closed")


def products_to_dataframe(products: List[Dict]) -> pl.DataFrame:
    """
    Convert product list to DataFrame with standardized schema.

    Args:
        products: List of product dictionaries from API

    Returns:
        DataFrame with products
    """
    if not products:
        return pl.DataFrame()

    df = pl.DataFrame(products)

    # Select relevant columns, rename for consistency
    columns_mapping = {
        'id': 'product_id',
        'title': 'product_name',
        'price': 'product_price',
        'category': 'category',
        'stock': 'stock_quantity'
    }

    # Keep only mapped columns if they exist
    available_cols = [column for column in columns_mapping.keys() if column in df.columns]
    df = df.select(available_cols)
    df = df.rename({column: columns_mapping[column] for column in available_cols})

    logger.info(f"Converted {len(df)} products to DataFrame")
    return df


def add_bronze_metadata(df: pl.DataFrame, source: str = "dummyjson_api") -> pl.DataFrame:
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


def fetch_and_prepare_products(
    base_url: str = "https://dummyjson.com"
) -> pl.DataFrame:
    """
    End-to-end product API ingestion pipeline.

    Args:
        base_url: API base URL

    Returns:
        Ready-to-load DataFrame with Bronze metadata
    """
    logger.info("Starting products API ingestion pipeline")

    client = ProductAPIClient(base_url=base_url)

    try:
        # Fetch all products with pagination
        products = client.fetch_all_products(batch_size=100)

        # Convert to DataFrame
        df = products_to_dataframe(products)

        # Add metadata
        df = add_bronze_metadata(df, source="product_api")

        logger.info(f"Products pipeline complete: {len(df)} records ready")
        return df

    finally:
        client.close()


if __name__ == "__main__":
    # Example usage
    import os

    try:
        products_df = fetch_and_prepare_products()
        output_path = write_local_bronze(
            products_df,
            source="products",
            base_path=os.getenv("LOCAL_DATA_LAKE_PATH", DEFAULT_LOCAL_DATA_LAKE_PATH),
            file_format=os.getenv("LOCAL_STORAGE_FORMAT", "delta"),
        )
        print(f"\nFetched {len(products_df)} products")
        print(f"Stored Bronze data locally: {output_path}")
        print(f"Columns: {list(products_df.columns)}")
        print("\nFirst few rows:")
        print(products_df.head())
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
