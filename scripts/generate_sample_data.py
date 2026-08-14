"""Generate sample data for local development and testing."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def generate_sample_orders(n_records=1000, output_path="data/olist_orders.csv"):
    """
    Generate synthetic orders data similar to Olist dataset.

    Args:
        n_records: Number of order records to generate
        output_path: Path to save CSV file
    """
    logger.info(f"Generating {n_records} sample order records")

    np.random.seed(42)
    start_date = datetime(2024, 1, 1)

    orders = {
        'order_id': [f'ORD{i:06d}' for i in range(1, n_records + 1)],
        'customer_id': [f'CUST{np.random.randint(1, 500):05d}' for _ in range(n_records)],
        'order_date': [
            (start_date + timedelta(days=np.random.randint(0, 90))).strftime('%Y-%m-%d')
            for _ in range(n_records)
        ],
        'total_price': np.random.uniform(20, 500, n_records),
        'state': np.random.choice(['SP', 'RJ', 'MG', 'BA', 'SC', 'RS', 'PE'], n_records)
    }

    df = pd.DataFrame(orders)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    logger.info(f"Successfully generated orders data: {output_path}")
    return df


def generate_sample_reviews(n_records=800, output_path="data/olist_reviews.csv"):
    """
    Generate synthetic reviews data.

    Args:
        n_records: Number of review records to generate
        output_path: Path to save CSV file
    """
    logger.info(f"Generating {n_records} sample review records")

    np.random.seed(42)

    sample_comments = [
        "Great product, fast delivery!",
        "Excellent quality, highly recommend",
        "Broke after a week, very disappointed",
        "Not as described, poor packaging",
        "Amazing! Perfect gift",
        "Late delivery, but product is good",
        "Defective item, refund process was difficult",
        "Best purchase ever!",
        "Waste of money, terrible quality",
        "Fine product, nothing special",
        "Packaging was damaged",
        "Exactly what I expected, very happy",
    ]

    start_date = datetime(2024, 1, 1)

    reviews = {
        'review_id': [f'REV{i:06d}' for i in range(1, n_records + 1)],
        'order_id': [f'ORD{np.random.randint(1, 1001):06d}' for _ in range(n_records)],
        'product_id': [f'PROD{np.random.randint(1, 101):05d}' for _ in range(n_records)],
        'review_comment': np.random.choice(sample_comments, n_records),
        'review_creation_date': [
            (start_date + timedelta(days=np.random.randint(0, 90))).strftime('%Y-%m-%d')
            for _ in range(n_records)
        ]
    }

    df = pd.DataFrame(reviews)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    logger.info(f"Successfully generated reviews data: {output_path}")
    return df


def generate_sample_products(n_records=100, output_path="data/products.csv"):
    """
    Generate synthetic products reference data.

    Args:
        n_records: Number of product records to generate
        output_path: Path to save CSV file
    """
    logger.info(f"Generating {n_records} sample product records")

    np.random.seed(42)

    categories = ['Electronics', 'Home & Garden', 'Sports', 'Books', 'Fashion', 'Beauty']

    products = {
        'product_id': [f'PROD{i:05d}' for i in range(1, n_records + 1)],
        'product_name': [f'Product {i}' for i in range(1, n_records + 1)],
        'category': np.random.choice(categories, n_records),
        'price': np.random.uniform(10, 500, n_records),
        'stock_quantity': np.random.randint(0, 500, n_records)
    }

    df = pd.DataFrame(products)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    logger.info(f"Successfully generated products data: {output_path}")
    return df


if __name__ == "__main__":
    logger.info("Starting sample data generation")

    try:
        generate_sample_orders(n_records=1000)
        generate_sample_reviews(n_records=800)
        generate_sample_products(n_records=100)

        logger.info("✅ Sample data generation complete!")
        print("\n" + "="*50)
        print("Sample data generated successfully!")
        print("="*50)
        print("Files created:")
        print("  - data/olist_orders.csv (1000 records)")
        print("  - data/olist_reviews.csv (800 records)")
        print("  - data/products.csv (100 records)")
        print("\nNext steps:")
        print("  1. Set up environment: cp .env.example .env")
        print("  2. Install dependencies: pip install -r requirements.txt")
        print("  3. Start PostgreSQL: docker-compose up -d")
        print("  4. Test ingestion: python src/ingestion/orders_loader.py")

    except Exception as e:
        logger.error(f"Data generation failed: {str(e)}")
        sys.exit(1)
