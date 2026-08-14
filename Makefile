SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help sync sync-dev lock test test-cov flow cloud-status cloud-login cloud-use flow-cloud ingest sentiment silver gold lint format typecheck

help:
	@echo "Available targets:"
	@echo "  make sync       - Install runtime dependencies from uv.lock"
	@echo "  make sync-dev   - Install runtime + dev dependencies"
	@echo "  make lock       - Refresh uv.lock from pyproject.toml"
	@echo "  make test       - Run unit tests"
	@echo "  make test-cov   - Run tests with coverage report"
	@echo "  make flow       - Run full Prefect flow"
	@echo "  make cloud-status - Show Prefect profile and API configuration"
	@echo "  make cloud-login  - Login to Prefect Cloud"
	@echo "  make cloud-use    - Switch to PREFECT_PROFILE (default: ecommerce-cloud)"
	@echo "  make flow-cloud   - Use cloud profile, then run full Prefect flow"
	@echo "  make ingest     - Run ingestion scripts"
	@echo "  make sentiment  - Run sentiment enrichment"
	@echo "  make silver     - Run Bronze to Silver pipeline"
	@echo "  make gold       - Run Silver to Gold pipeline"
	@echo "  make lint       - Run flake8"
	@echo "  make format     - Run black + isort"
	@echo "  make typecheck  - Run mypy"

sync:
	uv sync

sync-dev:
	uv sync --extra dev

lock:
	uv lock

test:
	uv run pytest tests/unit -q

test-cov:
	uv run pytest tests --cov=src --cov-report=html

flow:
	uv run python src/flows/prefect_flow.py

cloud-status:
	prefect profile ls
	prefect config view

cloud-login:
	prefect cloud login

cloud-use:
	prefect profile use $${PREFECT_PROFILE:-ecommerce-cloud}

flow-cloud:
	prefect profile use $${PREFECT_PROFILE:-ecommerce-cloud}
	uv run python src/flows/prefect_flow.py

ingest:
	uv run python src/ingestion/orders_loader.py
	uv run python src/ingestion/reviews_loader.py
	uv run python src/ingestion/product_api_client.py

sentiment:
	uv run python src/ingestion/sentiment_enrichment.py

silver:
	uv run python src/transform/silver_layer_loader.py

gold:
	uv run python src/transform/gold_layer_loader.py

lint:
	uv run flake8 src tests

format:
	uv run isort src tests
	uv run black src tests

typecheck:
	uv run mypy src
