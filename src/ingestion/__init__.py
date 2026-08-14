"""Ingestion module for loading data from various sources into Bronze layer."""

from .orders_loader import load_and_prepare_orders
from .product_api_client import fetch_and_prepare_products
from .reviews_loader import load_and_prepare_reviews

__all__ = [
    "load_and_prepare_orders",
    "fetch_and_prepare_products",
    "load_and_prepare_reviews",
]
