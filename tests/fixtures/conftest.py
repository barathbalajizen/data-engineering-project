"""Shared fixtures for integration and unit tests."""

import pytest
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_api_response():
    """Mock API response for product data."""
    return {
        "products": [
            {
                "id": 1,
                "title": "Laptop",
                "category": "Electronics",
                "price": 999.99,
                "stock": 10
            },
            {
                "id": 2,
                "title": "Mouse",
                "category": "Accessories",
                "price": 29.99,
                "stock": 50
            },
            {
                "id": 3,
                "title": "Monitor",
                "category": "Electronics",
                "price": 299.99,
                "stock": 25
            }
        ],
        "total": 3,
        "skip": 0,
        "limit": 100
    }


@pytest.fixture
def fixture_directory():
    """Return path to fixtures directory."""
    return Path(__file__).parent
