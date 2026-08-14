# E-Commerce Sales & Customer Feedback Pipeline

An end-to-end data engineering project that ingests order data, product catalogs, and customer reviews from multiple sources, processes them through a medallion architecture (Bronze/Silver/Gold), and powers a real-time sales and sentiment dashboard.

**Status**: ✅ **COMPLETE** - All 8 phases implemented and tested (51/51 tests passing)

---

## 🎯 Project Overview

### Business Problem
A retailer's data lives in three disconnected places:
- **Daily order exports** (CSV) — sales data
- **Product catalog API** (JSON) — product info & prices  
- **Customer reviews** (unstructured text) — feedback

**Solution**: An automated daily pipeline that:
1. ✅ Ingests all three sources (Phase 1)
2. ✅ Extracts sentiment from review text (Phase 2)
3. ✅ Transforms raw → cleaned → modeled data (Phases 3-4)
4. ✅ Validates quality at every stage (Phase 6)
5. ✅ Orchestrates with Prefect + scheduling (Phase 5)
6. ✅ Containerizes with Docker + CI/CD (Phase 7)
7. ✅ Powers a Streamlit dashboard (Phase 8)

---

## 📊 Architecture

```
INGESTION (Phase 1)          EXTRACTION (Phase 2)        TRANSFORMATION
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│ Orders CSV       │         │ Reviews CSV      │         │ SENTIMENT ANALYSIS│
│ (daily batch)    │ ───────→│ (raw feedback)   │ ───────→│ (VADER)           │
└──────────────────┘         └──────────────────┘         └──────────────────┘
                                                                     │
┌──────────────────┐                                                 ↓
│ Product API      │                                         ┌──────────────────┐
│ (JSON REST)      │ ────────────────────────────────────────│ BRONZE LAYER     │
│ (DummyJSON)      │                                         │ (as-is, raw)     │
└──────────────────┘                                         └──────────────────┘
                                                                     │
                        Phases 3-4: Bronze → Silver → Gold          │
                        ┌─────────────────────────────────┐         │
                        │ SILVER LAYER (cleaned, typed)   │←────────┘
                        │ - Standardize formats           │
                        │ - Deduplicate orders            │
                        │ - Join reviews to orders        │
                        │ - Validate data quality         │
                        └─────────────────────────────────┘
                                    │
                                    ↓
                        ┌─────────────────────────────────┐
                        │ GOLD LAYER (star schema)        │
                        │ - fact_orders (line items)      │
                        │ - dim_product (who/what)        │
                        │ - dim_customer (who)            │
                        │ - dim_date (when)               │
                        └─────────────────────────────────┘
                                    │
                                    ↓
                        ┌─────────────────────────────────┐
                        │ PostgreSQL (serving layer)      │
                        │ + Streamlit Dashboard           │
                        └─────────────────────────────────┘
```

**Storage**: Azure Blob Storage / ADLS Gen2 (Bronze/Silver/Gold as Delta Lake)  
**Compute**: Python + Polars + Prefect  
**Database**: PostgreSQL (or Azure SQL)  
**Orchestration**: Prefect (scheduling, retries, alerting)  
**CI/CD**: GitHub Actions + Azure Container Registry  

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- uv (fast Python package manager)
- Docker & Docker Compose (for PostgreSQL)
- Git

### Setup (5 minutes)

1. **Clone repository & configure:**
   ```bash
   git clone <repo-url>
   cd ecommerce-pipeline
   cp .env.example .env
   ```

2. **Install dependencies (uv):**
   ```bash
   uv sync --extra dev
   ```

3. **Start PostgreSQL:**
   ```bash
   docker-compose up postgres pgadmin -d
   # Access pgAdmin: http://localhost:5050
   ```

4. **Download sample data:**
   ```bash
   # Download Olist dataset from Kaggle to data/
   # Or generate synthetic data:
   uv run python scripts/generate_sample_data.py
   ```

5. **Run pipeline:**
   ```bash
   uv run python -m src.flows.prefect_flow --run-date 2024-01-15
   ```

6. **View dashboard:**
   ```bash
   uv run streamlit run dashboard/app.py
   # Opens http://localhost:8501
   ```

---

## 📋 Project Phases

| Phase | Component | Status | Files |
|-------|-----------|--------|-------|
| **1** | Ingestion (CSV, API, text) | ✅ | `src/ingestion/*` |
| **2** | Sentiment extraction (VADER) | ✅ | `src/extraction/review_sentiment.py` |
| **3** | Bronze → Silver | ✅ | `src/transform/bronze_to_silver.py` |
| **4** | Silver → Gold (star schema) | ✅ | `src/transform/silver_to_gold.py` |
| **5** | Prefect orchestration + scheduling | ✅ | `src/flows/prefect_flow.py` |
| **6** | Testing (51 tests, mocked APIs, E2E) | ✅ | `tests/unit/*`, `tests/integration/*` |
| **7** | Docker + CI/CD (GH Actions, ACR) | ✅ | `Dockerfile`, `.github/workflows/*` |
| **8** | Streamlit dashboard + docs | ✅ | `dashboard/app.py`, `docs/*` |

---

## 🧪 Testing

### Run All Tests (51 tests)
```bash
uv run pytest tests/ -v
# Unit tests:           39 passing
# Integration tests:     5 passing  
# Mocked API tests:      7 passing
```

### Run Unit Tests Only
```bash
uv run pytest tests/unit/ -v --cov=src
```

### Run Integration Tests
```bash
uv run pytest tests/integration/ -v
```

### Run Specific Test
```bash
uv run pytest tests/unit/test_bronze_to_silver.py::test_standardize_orders -v
```

---

## 🐳 Docker

### Run Locally
```bash
# Start all services (Postgres + pgAdmin + optional Pipeline)
docker-compose --profile with-pipeline --profile with-pgadmin up -d

# View logs
docker-compose logs -f pipeline

# Stop services
docker-compose down
```

### Build & Deploy to Azure Container Registry
See [docs/DOCKER.md](docs/DOCKER.md) for full instructions:
- Building multi-stage Docker image
- Deploying to Azure Container Registry
- CI/CD pipeline with GitHub Actions

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) | Schema & data definitions (Bronze/Silver/Gold) |
| [DOCKER.md](docs/DOCKER.md) | Docker setup & deployment guide |
| [PREFECT_STEP_BY_STEP.md](docs/PREFECT_STEP_BY_STEP.md) | Prefect flow configuration |
| [RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md) | Production deployment checklist |

---

## 🔧 Configuration

### Environment Variables
See [.env.example](.env.example) for all options:
```bash
# Database
PG_HOST=localhost
PG_USER=postgres
PG_PASSWORD=postgres

# Data Lake
LOCAL_DATA_LAKE_PATH=C:/ecommerce_delta_lake

# Feature Flags
ENABLE_SENTIMENT_EXTRACTION=true
ENABLE_DATA_QUALITY_CHECKS=true
```

### pyproject.toml
Project metadata, dependencies, and test configuration:
```bash
uv run pytest          # Run tests
uv sync               # Install dependencies
uv pip list           # View installed packages
```

---

## 📊 Dashboard

Interactive Streamlit dashboard with:
- 📈 **Revenue trends** by date & category
- 🏆 **Top/Bottom products** by sales & sentiment
- 😊 **Sentiment distribution** pie & histogram
- ⚠️ **Flagged negative reviews** (quality alerts)

```bash
uv run streamlit run dashboard/app.py
```

Loads data from Gold layer (Delta Lake). Falls back to sample data in demo mode.

---

## 🎯 Resume Talking Points

After completing this project, you can speak to:

### 🏗️ Architecture
- "Designed a medallion architecture (Bronze/Silver/Gold) for data quality progression"
- "Implemented dimensional modeling with surrogate keys and slowly changing dimensions"
- "Handled data from three different formats: CSV batch, REST API JSON, unstructured text"

### 🔄 ETL/ELT
- "Built transformation pipeline with Polars for type casting, deduplication, and joins"
- "Applied business logic: sentiment analysis, keyword flagging, data validation"
- "Scheduled daily runs with Prefect, including backfill capability for missed dates"

### 🧪 Quality & Testing
- "Implemented 51 automated tests: unit, integration, and mocked API calls"
- "Added data quality checks (schema validation, null checks, referential integrity)"
- "Achieved 100% test pass rate with continuous integration on every PR"

### 🐳 DevOps
- "Containerized pipeline with multi-stage Docker build (optimized for production)"
- "Implemented CI/CD with GitHub Actions: lint → test → build → deploy"
- "Deployed to Azure Container Registry with automated image scanning"

### 📊 Analytics
- "Built interactive Streamlit dashboard powered by dimensional model"
- "Enabled self-service analytics with SQL queries against Gold layer"
- "Surfaced data quality issues automatically (flagged negative reviews)"

---

## 🚨 Common Issues

**Q: Tests fail with "griffe < 1" error**  
A: Prefect 2.14.1 conflicts with griffe 2.x. Run: `pip install 'griffe<1'`

**Q: Docker container can't connect to PostgreSQL**  
A: Use `postgres` as hostname (Docker network), not `localhost`. Check docker network: `docker network inspect ecommerce-network`

**Q: Delta Lake table not found after running pipeline**  
A: Ensure LOCAL_DATA_LAKE_PATH environment variable is set and writable

**Q: Streamlit dashboard shows no data**  
A: Dashboard defaults to sample data if Gold tables unavailable. Run full pipeline first or check Delta Lake path

---

## 📈 Next Steps (Scaling)

If building this for production, consider:

- **Cloud Storage**: Use Azure ADLS Gen2 instead of local file system
- **Compute**: Migrate Python to Apache Spark (PySpark) for larger datasets
- **Warehouse**: Use Azure Synapse or Snowflake for SQL queries
- **Orchestration**: Add Apache Airflow or Databricks Workflows
- **Monitoring**: Integrate with Datadog/New Relic + PagerDuty for alerts
- **Data Catalog**: Add metadata tracking (Unity Catalog, Collibra)
- **ML**: Add demand forecasting or recommendation engine on top

---

## 📝 License

MIT - See LICENSE file

---

## 👤 Author

Created as a portfolio project demonstrating real-world data engineering practices.

**Questions?** Open an issue or contact the maintainer.

---

**Last Updated**: August 2024  
**Pipeline Status**: ✅ Production-Ready


# Run the full Prefect flow
uv run python src/flows/prefect_flow.py

# Run unit tests
uv run pytest tests/unit -q

# Add a new dependency and re-lock
uv add <package-name>
uv lock
```

### Automation Shortcuts

- Shortcut targets: [Makefile](Makefile)
- Windows task runner: [scripts/tasks.ps1](scripts/tasks.ps1)
- CI workflow: [.github/workflows/ci.yml](.github/workflows/ci.yml)
- Release runbook: [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md)

Common targets from [Makefile](Makefile):

```bash
make sync-dev
make test
make flow
make flow-cloud
```

Windows PowerShell shortcuts:

```powershell
.\scripts\tasks.ps1 sync-dev
.\scripts\tasks.ps1 test
.\scripts\tasks.ps1 flow
.\scripts\tasks.ps1 flow-cloud
```

Prefect Cloud helpers:

```powershell
.\scripts\tasks.ps1 cloud-status
.\scripts\tasks.ps1 cloud-login
.\scripts\tasks.ps1 cloud-use
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────┐
│  DATA SOURCES                           │
├─────────────────────────────────────────┤
│ • Orders (CSV)                          │
│ • Products (REST API)                   │
│ • Reviews (Unstructured Text)           │
└──────────────────┬──────────────────────┘
                   │
        ┌──────────▼──────────┐
        │  BRONZE (Raw Data)  │
        │  Raw landing zone   │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │ SILVER (Cleaned)    │
        │ Typed, deduplicated │
        │ Sentiment extracted │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │  GOLD (Analytics)   │
        │ Star schema + SQL   │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │   Dashboard         │
        │   Streamlit         │
        └─────────────────────┘
```

## 📊 What's Included

### Phase 0 - Setup ✅
- [x] Project structure
- [x] Environment configuration
- [x] Star schema DDL
- [x] Logging utility

### Phase 1 - Ingestion ✅
- [x] Orders CSV loader with retry logic
- [x] Product REST API client (DummyJSON)
- [x] Reviews loader with validation
- [x] Bronze layer partitioning strategy

### Phase 2 - Coming Soon
- [ ] Sentiment analysis (TextBlob/VADER)
- [ ] Keyword extraction from reviews
- [ ] Silver layer transformations

## 🚀 Usage

### Run Ingestion Pipeline

```bash
# Load orders from CSV
uv run python src/ingestion/orders_loader.py

# Fetch products from API
uv run python src/ingestion/product_api_client.py

# Load reviews from CSV
uv run python src/ingestion/reviews_loader.py
```

### Run Tests

```bash
# Unit tests only
uv run pytest tests/unit/

# With coverage
uv run pytest tests/ --cov=src/
```

## 📝 Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Ingestion | Python + requests | Direct, familiar |
| Transformation | Pandas + Polars | Industry standard |
| Storage | Local filesystem now, Azure ADLS Gen2 later | Works without a cloud account while preserving medallion layers |
| Orchestration | Prefect | Professional workflow management |
| Data Quality | Pandera | Schema validation at every stage |
| Database | PostgreSQL | Reliable OLTP serving layer |
| Dashboard | Streamlit | Fast prototyping and deployment |
| CI/CD | GitHub Actions | Free and integrated |
| Testing | pytest | Standard Python testing |

## 📚 Project Structure

```
ecommerce-pipeline/
├── config/              # Configuration files
├── src/
│   ├── ingestion/       # Data loading modules
│   ├── extraction/      # Sentiment & keyword extraction
│   ├── transform/       # Bronze→Silver→Gold transformations
│   ├── quality/         # Data quality checks
│   ├── storage/         # Local/Azure storage helpers
│   ├── db/              # Database models and loaders
│   └── utils/           # Logging and utilities
├── flows/               # Prefect orchestration
├── tests/               # Unit and integration tests
├── sql/                 # SQL DDL and queries
├── dashboard/           # Streamlit app
├── docker/              # Docker configuration
├── docs/                # Documentation
└── README.md            # This file
```

## 🔄 Data Flow

### Orders
```
CSV File → Load → Validate Schema → Standardize Types → Bronze (local CSV)
```

### Products
```
REST API → Fetch (with retry) → Parse JSON → DataFrame → Bronze (local CSV)
```

### Reviews
```
CSV File → Load → Clean Text → Validate → Bronze (local CSV) →
Extract Sentiment → Silver (local CSV)
```

## ✅ Quality Checks

Data quality is enforced at multiple stages:

- **Ingestion**: Schema validation, null checks
- **Transformation**: Data type consistency, range validation
- **Loading**: Deduplication, referential integrity

## 🧪 Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/unit/test_orders_loader.py

# Run with verbose output
uv run pytest -v

# Generate coverage report
uv run pytest --cov=src/ --cov-report=html
```

## 🔐 Environment Variables

See `.env.example` for all available options:

```bash
STORAGE_PROVIDER=local
LOCAL_DATA_LAKE_PATH=C:/ecommerce_delta_lake
LOCAL_STORAGE_FORMAT=delta
DB_HOST=localhost
DB_PORT=5432
LOG_LEVEL=INFO
```

## 📖 Key Concepts Demonstrated

- **Medallion Architecture**: Bronze (raw) → Silver (clean) → Gold (analytics)
- **Data Validation**: Pandera schema validation across pipeline stages
- **Unstructured Data**: Sentiment extraction from free-text reviews
- **Star Schema**: Dimensional modeling for analytics (fact_orders + dimensions)
- **Idempotency**: Safe re-runs with deduplication
- **Retry Logic**: Resilient API calls with exponential backoff
- **Logging**: Structured JSON logging for observability
- **Testing**: Unit tests with mocked dependencies

## 🎓 Interview Topics This Covers

- ETL pipeline design and implementation
- Handling multiple data formats and sources
- Data quality and validation strategies
- Dimensional modeling for analytics
- Orchestration and scheduling
- Containerization and CI/CD
- Testing strategies for data pipelines
- How would you scale to 500 stores?
- How do you handle pipeline failures?
- What metrics would you monitor in production?

## 📄 License

MIT License - see LICENSE file for details

## 👤 Contact

For questions or feedback about this project, please open an issue on GitHub.

---

**Status**: Phase 0 & 1 Complete ✅ | Next: Phase 2 (Sentiment Extraction)
