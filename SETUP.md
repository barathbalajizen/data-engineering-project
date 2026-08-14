"""Quick setup guide and checklist."""

# E-Commerce Data Pipeline - Complete Setup Guide

**Welcome!** This guide will walk you through running a complete 3-phase data engineering pipeline on your local machine. Follow each step carefully, and you'll have working data transformations from Bronze â†’ Silver â†’ Gold.

**Time Required:** ~30 minutes total (setup + running all phases)  
**Prerequisites:** Python 3.10, Docker, Git (optional)

---

## What This Project Does

```
Orders (CSV) â†’ Reviews (CSV) â†’ Products (API)
        â†“              â†“              â†“
    PHASE 1: INGESTION (Load raw data into Bronze layer)
        â†“
    PHASE 2: SENTIMENT EXTRACTION (Enrich reviews with sentiment scores)
        â†“
    PHASE 3: TRANSFORMATION (Clean, type, deduplicate, join into Silver)
        â†“
    SILVER LAYER: Ready for analytics/Gold layer
```

---

# Phase 0 & 1 Setup Checklist

## Completed
- Completed Project folder structure created
- Completed Environment configuration template (.env.example)
- Completed Dependencies list (requirements.txt)
- Completed Star schema DDL for PostgreSQL
- Completed Logging utility module
- Completed Orders loader with retry logic
- Completed Product API client (DummyJSON)
- Completed Reviews loader with validation
- Completed Unit tests for orders loader
- Completed Docker Compose setup (PostgreSQL + PgAdmin)
- Completed Sample data generation script
- Completed README with quick start guide

---

## ðŸ“‹ Prerequisites

Before you start, make sure you have:

- **Python 3.10** installed  
- **Docker & Docker Compose** installed and running  
- **Git** (optional, for version control)  
- **At least 5GB free disk space** (for Delta Lake data + Docker containers)  
- **Internet connection** (to fetch products from DummyJSON API)

**To check Python version:**
```powershell
python --version
# Should show Python 3.10.x
```

**To check Docker is running:**
```powershell
docker -v
docker-compose -v
```

## UV Quick Commands (Windows)

Use these commands for day-to-day work:

```powershell
# Sync environment from lockfile
uv sync --extra dev

# Run tests
uv run pytest tests/unit -q

# Run complete Prefect flow
uv run python src/flows/prefect_flow.py

# Add dependency and refresh lock
uv add <package-name>
uv lock
```

Windows PowerShell task runner (same operations as Makefile):

```powershell
.\scripts\tasks.ps1 help
.\scripts\tasks.ps1 sync-dev
.\scripts\tasks.ps1 test
.\scripts\tasks.ps1 flow
.\scripts\tasks.ps1 cloud-status
.\scripts\tasks.ps1 cloud-login
.\scripts\tasks.ps1 cloud-use
.\scripts\tasks.ps1 flow-cloud
```

Release readiness checklist:

- [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md)

---

## Next Steps (Do This Now)

### 1. Install Dependencies (2 minutes)

**What this does:** Creates a uv-managed virtual environment and installs all required packages from the lock file.

**Important on Windows:** Use `uv run ...` to ensure the correct environment is used.

```powershell
# Step 1.1: Open PowerShell and navigate to project directory
cd "c:\Data Engineer\Project"

# Step 1.2: Sync runtime + dev dependencies
# This installs: pandas, polars, deltalake, prefect, pytest, etc.
uv sync --extra dev
```

**Expected output:**
```
Resolved ... packages
Installed ... packages
```

**Success:** `.venv` created and all dependencies installed.

---

### 2. Setup Environment (1 minute)

**What this does:** Creates a `.env` file with default settings for data storage paths.

```bash
# Step 2.1: Copy the environment template
copy .env.example .env

# Step 2.2: Review the file (optional, defaults are fine for local dev)
# The defaults are:
#   - LOCAL_DATA_LAKE_PATH=C:/ecommerce_delta_lake (no spaces - required on Windows!)
#   - LOCAL_STORAGE_FORMAT=delta (using Delta Lake for versioning)
#   - Azure settings can stay blank for local development
```

**âš ï¸ Important:** Never use paths with spaces (e.g., "C:\Data Engineer\...") for Delta Lake tables on Windows. That's why we use `C:/ecommerce_delta_lake`.

**Success:** `.env` file created with correct settings.

---

### 3. Generate Sample Data (1 minute)

**What this does:** Creates sample CSV files that we'll use as data sources.

```bash
# Step 3.1: Run the sample data generator
# This creates: data/olist_orders.csv (1000 rows), data/olist_reviews.csv (800 rows), data/products.csv (100 rows)
uv run python scripts/generate_sample_data.py
```

**Expected output:**
```
Sample data generated successfully!
Files created:
  - data/olist_orders.csv (1000 records)
  - data/olist_reviews.csv (800 records)
  - data/products.csv (100 records)
```

**Success:** Sample data files created in the `data/` folder.

---

### 4. Start PostgreSQL (1 minute)

**What this does:** Launches PostgreSQL database and PgAdmin (web UI) using Docker.

```bash
# Step 4.1: Start Docker containers defined in docker-compose.yml
# This starts: PostgreSQL (port 5432) + PgAdmin (port 5050)
docker-compose up -d

# Step 4.2: Check status - both should show "Up"
docker-compose ps

# Step 4.3: Access PgAdmin web interface (optional)
# Open browser: http://localhost:5050
# Login: admin@example.com / admin
```

**Expected output from `docker-compose ps`:**
```
NAME                    STATUS
ecommerce-postgres      Up 2 minutes
ecommerce-pgadmin       Up 2 minutes
```

**Success:** PostgreSQL container running, accessible on port 5432.

---

### 5. Initialize Database (1 minute)

**What this does:** Creates the database schema (tables, views, indexes).

```bash
# Step 5.1: Run the DDL script to create schema
# This creates: fact_orders, dim_product, dim_customer, dim_date tables + views
docker exec ecommerce-postgres psql -U postgres -d ecommerce_db -f /docker-entrypoint-initdb.d/star_schema.sql

# Alternative: Use PgAdmin GUI
# 1. Open http://localhost:5050 in browser
# 2. Login with admin@example.com / admin
# 3. Servers â†’ ecommerce-postgres â†’ Databases â†’ ecommerce_db â†’ Query Tool
# 4. Paste contents of sql/ddl/star_schema.sql and execute
```

**Expected output:**
```
CREATE TABLE
CREATE TABLE
... (more DDL statements)
```

**Success:** Database schema created. Tables visible in PgAdmin.

---

### 6. Test Phase 1 Ingestion (5 minutes)

**What this does:** Loads data from 3 sources into the Bronze layer (Delta tables).

```bash
# Step 6.1: Run orders loader
# Reads: data/olist_orders.csv
# Writes: C:/ecommerce_delta_lake/bronze/orders/ (Delta format)
# Result: 1000 orders with standardized types, metadata, retry logic
uv run python src/ingestion/orders_loader.py

# Step 6.2: Run product API client
# Fetches: 194 products from https://dummyjson.com/products (with pagination)
# Writes: C:/ecommerce_delta_lake/bronze/products/ (Delta format)
# Result: Products with ID, name, price, stock
uv run python src/ingestion/product_api_client.py

# Step 6.3: Run reviews loader
# Reads: data/olist_reviews.csv
# Writes: C:/ecommerce_delta_lake/bronze/reviews/ (Delta format)
# Result: 800 reviews with standardized columns, metadata
uv run python src/ingestion/reviews_loader.py
```

**Expected output for each:**
```
2026-08-12 21:03:57,... - INFO - Starting [module] pipeline
2026-08-12 21:03:57,... - INFO - Successfully loaded/fetched X records
2026-08-12 21:03:57,... - INFO - Wrote X records to local bronze storage: C:\ecommerce_delta_lake\bronze\[source]
```

**Success:** Bronze Delta tables created at `C:/ecommerce_delta_lake/bronze/` containing:
- orders/ (1000 rows)
- reviews/ (800 rows)
- products/ (194 rows)

Each table has a `_delta_log/` folder proving Delta Lake format.

---

### 7. Run Unit Tests

**What this does:** Validates that all Phase 1 ingestion logic works correctly.

```bash
# Step 7.1: Run just Phase 1 tests (fast)
uv run pytest tests/unit/test_orders_loader.py -v

# Step 7.2: Run ALL tests including Phase 1 + Phase 2 + Phase 3
uv run pytest tests/unit/ -v

# Step 7.3: Run with coverage report (shows % of code tested)
uv run pytest tests/ --cov=src/ --cov-report=html
# Then open: htmlcov/index.html in browser to see coverage
```

**Expected output:**
```
tests/unit/test_orders_loader.py::test_validate_orders_schema_valid PASSED
tests/unit/test_orders_loader.py::test_standardize_orders_data PASSED
... (6 Phase 1 tests)
tests/unit/test_review_sentiment.py::test_extract_review_signals_positive_review PASSED
... (6 Phase 2 tests)
tests/unit/test_bronze_to_silver.py::test_standardize_orders PASSED
... (11 Phase 3 tests)

============================= 24 passed in 1.38s ==============================
```

**Success:** All 24 tests passing = pipeline working correctly!

---

## ðŸ“ What Each Phase 1 Module Does

### orders_loader.py
- Loads orders from CSV file
- Validates schema (required columns)
- Standardizes data types and formats
- Adds Bronze layer metadata (timestamps, source)
- Writes Bronze data to local storage
- Includes retry logic for resilience

**Key Features:**
- Retry mechanism with exponential backoff
- Type conversion (dates, prices)
- State column standardization (uppercase)
- Null handling

### product_api_client.py
- Fetches product catalog from REST API
- Handles pagination automatically
- Converts API response to DataFrame
- Writes Bronze data to local storage
- Includes retry logic with exponential backoff

**Key Features:**
- DummyJSON API integration (free, no auth)
- Paginated fetching
- JSON to DataFrame conversion
- Timeout and error handling

### reviews_loader.py
- Loads customer reviews from CSV
- Standardizes column names
- Validates required fields
- Cleans empty reviews
- Adds Bronze layer metadata
- Writes Bronze data to local storage

**Key Features:**
- Flexible column naming (handles variations)
- Empty review filtering
- String standardization
- Date parsing

## ðŸ”Ž Phase 2: Unstructured Review Extraction

### review_sentiment.py
- Extracts sentiment score from review text using VADER
- Labels reviews as positive, neutral, or negative
- Flags operational issue keywords such as broken, late, refund, damaged, defective, and wrong
- Adds structured columns for downstream Silver processing

**Key Features:**
- Handles empty review text safely
- Handles non-English text without failing
- Detects all-caps negative feedback
- Includes unit tests for extraction edge cases

```bash
# Run Phase 2 extraction tests
uv run pytest tests/unit/test_review_sentiment.py -v
```

### How to Check Phase 2 is Working

**Option 1: Run Phase 2 Unit Tests** (fastest)
```bash
uv run pytest tests/unit/test_review_sentiment.py -v
# Should pass 6 tests: positive, empty, non-English, all-caps, DataFrame enrichment, missing column
```

**Option 2: Run on Real Sample Data** (see actual results)
```powershell
@'
from src.ingestion.reviews_loader import load_and_prepare_reviews
from src.extraction.review_sentiment import enrich_reviews_with_signals

reviews = load_and_prepare_reviews('./data/olist_reviews.csv')
enriched = enrich_reviews_with_signals(reviews)

print("Sample reviews with sentiment extraction:")
print(enriched[['review_id', 'sentiment_label', 'sentiment_score', 'flagged_keywords']].head(10))
print("\nSentiment breakdown:")
print(enriched['sentiment_label'].value_counts())
print(f"\nReviews with flagged keywords: {int(enriched['has_flagged_keywords'].sum())}")
'@ | uv run python -
```

**Option 3: Check the Log File**
```powershell
Get-Content .\logs\src_extraction_review_sentiment.log -Tail 10
```

**Option 4: Run All Unit Tests** (verify Phase 2 didn't break Phase 1)
```bash
uv run pytest tests/unit/ -v
# Should show 13 tests passing (7 from Phase 1 + 6 from Phase 2)
```

## ðŸ§ª Testing

All modules have unit tests:

```bash
# Run specific test
uv run pytest tests/unit/test_orders_loader.py -v

# Run all tests with coverage report
uv run pytest tests/ --cov=src/ --cov-report=html
# Open htmlcov/index.html in browser
```

## ðŸŽ¯ Success Checklist

### Phase 1 Checklist (Data Ingestion)

- Completed Load orders CSV and see data printed
- Completed Fetch products from API and see results
- Completed Load reviews from CSV
- Completed See local Bronze Delta tables under `C:/ecommerce_delta_lake/bronze/`
- Completed See proper logging messages (console + files)
- Completed Pass all unit tests
- Completed Connect to PostgreSQL via PgAdmin
- Completed See database tables created

### Phase 2 Checklist (Unstructured Review Extraction)

- Completed Extract sentiment score from review text using VADER
- Completed Label reviews as positive, neutral, or negative
- Completed Flag operational issue keywords (broken, late, refund, damaged, defective, wrong)
- Completed Handle edge cases (empty reviews, non-English text, all-caps)
- Completed Pass all Phase 2 unit tests
- Completed Enrich sample reviews and verify sentiment breakdown

## ðŸ“Š What's Ready for Phase 2

Once Phase 1 is working:
- Raw data can be loaded from all 3 sources
- Logging and monitoring in place
- Retry logic for resilience
- Basic validation working

Now we've completed:
- Sentiment analysis (VADER)
- Keyword extraction
- ðŸ”œ Bronze â†’ Silver transformations

## ðŸ“‹ Phase 3: Bronze â†’ Silver Transformations

**What Phase 3 will do:**
- Read Bronze Delta tables (orders, reviews, products)
- **Standardize & type data:** convert dates, currencies, IDs to proper types
- **Join reviews to orders:** link review sentiment/keywords back to order and product
- **Deduplicate orders:** handle accidental re-runs of the same day's file
- **Write Silver Delta tables:** clean, typed, joined data ready for analytics
- **Add quality checks:** validate no negative prices, null order IDs, valid dates using `pandera`
- **Track lineage:** which Bronze file version produced this Silver record

**Key modules you'll build:**
- `src/transform/bronze_to_silver.py` â€” read Bronze, standardize, join, dedupe, write Silver
- `src/quality/checks.py` â€” `pandera` schema definitions for Silver layer validation
- `tests/unit/test_bronze_to_silver.py` â€” test transformations on fixtures

**Why it matters:**
- Bridge between raw (Bronze) and analytics-ready (Silver) data
- Where most bugs hide: joins, type mismatches, silent NULL handling
- This is what 70% of production pipelines spend time on

**Getting ready for Phase 3:**
- You already have Bronze Delta tables at `C:/ecommerce_delta_lake/bronze/{orders,reviews,products}`
- You know how to read Delta tables with `deltalake.DeltaTable`
- You know how to add columns with sentiment/keywords (Phase 2)
- Next: combine those plus joins + validation

---

## Phase 3 Implementation (Bronze to Silver Transformations)

**Status: ðŸŽ‰ COMPLETE**

Phase 3 reads Bronze data, cleans it, joins it, validates quality, and writes clean Silver tables ready for analytics.

### Phase 3 Complete Step-by-Step Guide

**Prerequisites:** You must have completed Phase 1 & 2 first!

---

#### Step 1: Verify Bronze Tables Exist (1 minute)

Before running Phase 3, confirm that Phase 1 created Bronze tables.

```bash
# Check that Bronze directory exists with 3 tables
# Should show: C:\ecommerce_delta_lake\bronze\orders\
#              C:\ecommerce_delta_lake\bronze\reviews\
#              C:\ecommerce_delta_lake\bronze\products\

# On PowerShell, list the directory:
ls C:\ecommerce_delta_lake\bronze\
```

**Expected:**
```
    Directory: C:\ecommerce_delta_lake\bronze

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----        8/12/2026  9:03 PM                orders
d-----        8/12/2026  9:04 PM                reviews
d-----        8/12/2026  9:04 PM                products
```

**Success:** Bronze tables exist.

---

#### Step 2: Enrich Reviews with Sentiment (2 minutes)

Phase 2 bridges Bronze ingestion with Phase 3 transformation. This reads reviews and adds sentiment scores using VADER.

```bash
# What it does:
# 1. Reads Bronze reviews from C:/ecommerce_delta_lake/bronze/reviews/
# 2. Extracts sentiment score from each review text (using VADER SentimentIntensityAnalyzer)
# 3. Adds 4 new columns: sentiment_score, sentiment_label, flagged_keywords, has_flagged_keywords
# 4. Rewrites Bronze reviews with enriched data
# 5. Reports: sentiment distribution (positive/negative), flagged keyword count

uv run python 
```

**Expected output:**
```
==================================================
Phase 2 Review Sentiment Extraction Complete!
==================================================
Total reviews enriched: 800
Sentiment distribution: {'positive': 470, 'negative': 330}
Flagged reviews: 322
Bronze path: C:\ecommerce_delta_lake\bronze\reviews
==================================================
```

**What happened:**
- 800 reviews were analyzed for sentiment
- 470 marked as positive (sentiment_score >= 0.05)
- 330 marked as negative (sentiment_score <= -0.05)
- 322 flagged as having issue keywords (broken, late, refund, damaged, etc.)

**Success:** Bronze reviews now enriched with sentiment.

---

#### Step 3: Run Phase 3 Transformation (2 minutes)

Now run the full Bronze â†’ Silver transformation pipeline.

```bash
# What it does:
# 1. Read Bronze tables: orders, reviews, products (Delta format)
# 2. Standardize data types:
#    - Convert dates to datetime
#    - Convert prices to float
#    - Uppercase state codes
# 3. Add missing columns (fill with defaults)
# 4. Deduplicate orders (keep most recent by timestamp)
# 5. Join reviews to orders (left outer join on order_id)
# 6. Validate quality:
#    - No negative prices
#    - No duplicate IDs
#    - Sentiment scores in [-1.0, 1.0] range
#    - Sentiment labels in {positive, neutral, negative}
# 7. Write 4 Silver tables:
#    - C:/ecommerce_delta_lake/silver/orders (1000 deduplicated records)
#    - C:/ecommerce_delta_lake/silver/reviews (800 records)
#    - C:/ecommerce_delta_lake/silver/products (194 records)
#    - C:/ecommerce_delta_lake/silver/orders_reviews (1268 joined records)

uv run python 
```

**Expected output:**
```
============================================================
PHASE 3: BRONZE â†’ SILVER TRANSFORMATION
============================================================
2026-08-12 21:06:24,755 - src.transform.bronze_to_silver - INFO - Starting Bronze â†’ Silver transformation
2026-08-12 21:06:24,755 - src.transform.bronze_to_silver - INFO - Reading Bronze table: C:\ecommerce_delta_lake\bronze\orders
2026-08-12 21:06:24,833 - src.transform.bronze_to_silver - INFO - Read 1000 records from Bronze orders
2026-08-12 21:06:24,833 - src.transform.bronze_to_silver - INFO - Reading Bronze table: C:\ecommerce_delta_lake\bronze\reviews
2026-08-12 21:06:24,889 - src.transform.bronze_to_silver - INFO - Read 800 records from Bronze reviews
2026-08-12 21:06:24,889 - src.transform.bronze_to_silver - INFO - Reading Bronze table: C:\ecommerce_delta_lake\bronze\products
2026-08-12 21:06:24,948 - src.transform.bronze_to_silver - INFO - Read 194 records from Bronze products
... (more logs)
2026-08-12 21:06:25,028 - src.transform.bronze_to_silver - INFO - Wrote 1000 records to Silver orders
2026-08-12 21:06:25,114 - src.transform.bronze_to_silver - INFO - Wrote 800 records to Silver reviews
2026-08-12 21:06:25,208 - src.transform.bronze_to_silver - INFO - Wrote 194 records to Silver products
2026-08-12 21:06:25,293 - src.transform.bronze_to_silver - INFO - Wrote 1268 records to Silver orders_reviews

Phase 3 Transformation Complete!
============================================================
Silver tables created:
  â€¢ orders: C:\ecommerce_delta_lake\silver\orders
  â€¢ reviews: C:\ecommerce_delta_lake\silver\reviews
  â€¢ products: C:\ecommerce_delta_lake\silver\products
  â€¢ orders_reviews: C:\ecommerce_delta_lake\silver\orders_reviews
============================================================
```

**What this means:**
- Bronze tables successfully read
- Data standardized and typed
- Quality validation passed (no errors)
- All Silver tables created and ready

**Success:** Phase 3 complete! Silver layer created.

---

#### Step 4: Verify Silver Tables Were Created (1 minute)

Confirm the transformation worked by checking the output.

```bash
# Check Silver directory exists with 4 tables
ls C:\ecommerce_delta_lake\silver\
```

**Expected:**
```
    Directory: C:\ecommerce_delta_lake\silver

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----        8/12/2026  9:06 PM                orders
d-----        8/12/2026  9:06 PM                reviews
d-----        8/12/2026  9:06 PM                products
d-----        8/12/2026  9:06 PM                orders_reviews
```

---

#### Step 5: Run Tests to Verify Everything Works (1 minute)

```bash
# Run just Phase 3 tests
uv run pytest tests/unit/test_bronze_to_silver.py -v
# Should show 11 tests passing

# Run ALL tests (Phase 1 + 2 + 3)
uv run pytest tests/unit/ -v
# Should show 24 tests passing total
```

**Expected output:**
```
tests/unit/test_bronze_to_silver.py::test_standardize_orders PASSED [  4%]
tests/unit/test_bronze_to_silver.py::test_standardize_reviews PASSED [  8%]
... (more Phase 3 tests)
tests/unit/test_review_sentiment.py::test_extract_review_signals_positive_review PASSED
... (Phase 2 tests)
tests/unit/test_orders_loader.py::test_validate_orders_schema_valid PASSED
... (Phase 1 tests)

============================= 24 passed in 1.38s ==============================
```

**Success:** All tests passing! Pipeline fully validated.

---

### Phase 3 Quick Start

**Step 1: Verify Phase 1 & 2 are working**
```bash
# Make sure Bronze tables exist at C:/ecommerce_delta_lake/bronze/
uv run python 
uv run python 
uv run python 
```

**Step 2: Enrich reviews with sentiment (Phase 2)**
```bash
uv run python 
```

**Step 3: Run Phase 3 transformation**
```bash
uv run python 
```

**Expected output:**
```
PHASE 3: BRONZE â†’ SILVER TRANSFORMATION
============================================================
Phase 3 Transformation Complete!
Silver tables created:
  â€¢ orders: C:\ecommerce_delta_lake\silver\orders
  â€¢ reviews: C:\ecommerce_delta_lake\silver\reviews
  â€¢ products: C:\ecommerce_delta_lake\silver\products
  â€¢ orders_reviews: C:\ecommerce_delta_lake\silver\orders_reviews
============================================================
```

### How to Check Phase 3 is Working

**Option 1: Run Phase 3 Unit Tests** (fastest - validates transformations)
```bash
# What this does:
# - Tests standardization (type conversion, uppercase state, etc.)
# - Tests deduplication (keeps most recent order)
# - Tests joining (merges reviews with orders correctly)
# - Tests validation (checks for negative prices, invalid dates, etc.)
# - Tests error cases (ensures invalid data is caught)

uv run pytest tests/unit/test_bronze_to_silver.py -v
# Should pass 11 tests: standardization, deduplication, joining, validation
```

**Option 2: Run on Real Sample Data** (see actual counts & stats)

Run this to verify the transformation results:

```powershell
@'
from deltalake import DeltaTable
from pathlib import Path

base_path = "C:/ecommerce_delta_lake"
print("\n" + "="*60)
print("SILVER LAYER VERIFICATION")
print("="*60)

for table_name in ["orders", "reviews", "products", "orders_reviews"]:
    silver_path = Path(base_path) / "silver" / table_name
    table_dt = DeltaTable(str(silver_path))
    df = table_dt.to_pandas()
    print(f"\nâœ“ {table_name.upper()}")
    print(f"   Records: {len(df):,}")
    print(f"   Columns: {df.shape[1]}")
    if "sentiment_label" in df.columns:
        print(f"   Sentiment: {df['sentiment_label'].value_counts().to_dict()}")
    if "total_price" in df.columns:
        print(f"   Price range: ${df['total_price'].min():.2f} - ${df['total_price'].max():.2f}")

print("\n" + "="*60)
print("All Silver tables verified! âœ“")
print("="*60 + "\n")
'@ | uv run python -
```

**Expected output:**
```
============================================================
SILVER LAYER VERIFICATION
============================================================

âœ“ ORDERS
   Records: 1,000
   Columns: 9
   Price range: $21.54 - $499.72

âœ“ REVIEWS
   Records: 800
   Columns: 12
   Sentiment: {'positive': 470, 'negative': 330}

âœ“ PRODUCTS
   Records: 194
   Columns: 8

âœ“ ORDERS_REVIEWS
   Records: 1,268
   Columns: 19
   Sentiment: {'positive': 470, 'negative': 330}
   Price range: $21.54 - $499.72

============================================================
All Silver tables verified! âœ“
============================================================
```

**Option 3: Check Phase 3 Log File**

View the log output to see what Phase 3 did:

```powershell
# Show last 20 lines of Phase 3 logs
Get-Content .\logs\src_transform_bronze_to_silver.log -Tail 20
```

**Option 4: Run All Unit Tests** (verify nothing broke)

Make sure all 3 phases still work together:

```bash
# Run complete test suite
uv run pytest tests/unit/ -v

# Expected:
# âœ“ 6 Phase 1 tests (ingestion)
# âœ“ 6 Phase 2 tests (sentiment extraction)
# âœ“ 11 Phase 3 tests (transformation)
# = 24 tests total passing
```

---

### Phase 3 Checklist (Data Standardization & Quality)

- Completed Read Bronze orders, reviews, products from Delta tables
- Completed Standardize data types (dates, prices, IDs to proper types)
- Completed Uppercase state codes
- Completed Handle missing sentiment columns in reviews
- Completed Deduplicate orders on order_id (keep most recent by timestamp)
- Completed Join reviews to orders with left outer join
- Completed Validate no negative prices
- Completed Validate unique order/review/product IDs
- Completed Validate sentiment scores in [-1.0, 1.0] range
- Completed Validate sentiment labels in {positive, neutral, negative}
- Completed Write 4 Silver Delta tables with proper partitioning
- Completed Add _ingestion_date partitioning to Silver tables
- Completed Pass all 11 Phase 3 unit tests
- Completed Run end-to-end smoke test on real data

### Phase 3 Key Modules

**src/quality/checks.py** (Data Quality Schemas)
```python
def validate_silver_orders(df)          # Check unique IDs, valid prices
def validate_silver_reviews(df)         # Check sentiment range & labels
def validate_silver_products(df)        # Check unique IDs, valid prices
def validate_silver_orders_reviews(df)  # Check joined data quality
```

**src/transform/bronze_to_silver.py** (Transformation Pipeline)
```python
def read_bronze_table(base_path, source)         # Read Delta â†’ pandas
def standardize_orders(df)                       # Type conversion, uppercase state
def standardize_reviews(df)                      # Type conversion, add default sentiment
def standardize_products(df)                     # Type conversion
def deduplicate_orders(df)                       # Sort by timestamp, drop_duplicates
def join_reviews_to_orders(orders_df, reviews_df) # Left outer merge
def write_silver_table(df, layer, source, ...)   # Validate & write Delta
def transform_bronze_to_silver(base_path, validate=True) # Orchestrate end-to-end
```

**src/ingestion/sentiment_enrichment.py** (Phase 2 Bridge)
```python
def enrich_bronze_reviews_with_sentiment(base_path) # Read Bronze, add sentiment, rewrite
```

### Phase 3 Results

With sample data (1000 orders, 800 reviews, 194 products):
- **Silver Orders:** 1000 records (deduplicated)
- **Silver Reviews:** 800 records (with sentiment & keywords)
- **Silver Products:** 194 records (type-standardized)
- **Silver Orders+Reviews:** 1268 records (left-joined: orders + matching reviews)

All tables partitioned by `_ingestion_date` for efficient querying.

---

## ðŸ†˜ Troubleshooting Guide

### Common Issues & Solutions

#### âŒ "ModuleNotFoundError: No module named 'src'"

**Problem:** Python can't find the project modules.

**Solution:**
```bash
# Make sure you're running from project root:
cd "c:\Data Engineer\Project"

# Confirm venv is activated (should see (venv) in prompt):
# If not, activate it:
venv\Scripts\activate
```

---

#### âŒ "PostgreSQL connection refused"

**Problem:** Docker containers not running.

**Solution:**
```bash
# Check if containers are running
docker-compose ps

# If not running, start them
docker-compose up -d

# Verify they're up (Status should be "Up")
docker-compose ps
```

---

#### âŒ "CSV file not found"

**Problem:** Sample data wasn't generated.

**Solution:**
```bash
# Generate sample data
uv run python 

# Verify files created:
ls data/
# Should show: olist_orders.csv, olist_reviews.csv, products.csv
```

---

#### âŒ "API timeout" (DummyJSON product fetch)

**Problem:** DummyJSON API is slow or unavailable.

**Solution:**
```bash
# The client has automatic retry logic (3 attempts with exponential backoff)
# Just run it again - it usually works on second attempt
uv run python 

# If still failing, check your internet connection
```

---

#### âŒ "Delta Lake path contains spaces" (Windows)

**Problem:** Path like `C:\Data Engineer\Project` causes read errors.

**Solution:**
```bash
# This is why we use C:/ecommerce_delta_lake (no spaces)
# Verify in .env file:
cat .env

# Should show: LOCAL_DATA_LAKE_PATH=C:/ecommerce_delta_lake
```

---

#### âŒ Tests fail with "Schema mismatch"

**Problem:** Old Delta tables have different schema than expected.

**Solution:**
```bash
# Delete the Delta Lake directory and regenerate
Remove-Item -Path "C:\ecommerce_delta_lake" -Recurse -Force

# Rerun Phase 1 ingestion
uv run python 
uv run python 
uv run python 

# Then rerun Phase 3
uv run python 
```

---

#### âŒ "ImportError: No module named 'polars'"

**Problem:** Polars not installed.

**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt

# Or specifically install polars
pip install polars==0.19.12
```

---

### Verify Your Setup

Run this quick test to verify everything is configured correctly:

```bash
# 1. Check Python version
python --version
# Should be Python 3.10.x

# 2. Check venv is active
# Should see (venv) in prompt

# 3. Check imports work
uv run python -c "import pandas, polars, deltalake; print('âœ“ All imports OK')"

# 4. Check Docker is running
docker ps
# Should show: ecommerce-postgres, ecommerce-pgadmin

# 5. Check sample data exists
ls data/
# Should show: olist_orders.csv, olist_reviews.csv, products.csv

# 6. Check .env file exists
ls .env
# Should exist
```

---

## âš¡ Quick Reference: Run Everything at Once

If you want to run the entire pipeline in one go (takes ~2 minutes):

```bash
# 1. Generate data
uv run python 

# 2. Phase 1: Load raw data
uv run python 
uv run python 
uv run python 

# 3. Phase 2: Add sentiment
uv run python 

# 4. Phase 3: Transform to Silver
uv run python 

# 5. Verify all tests pass
uv run pytest tests/unit/ -v
```

**Expected final output:**
```
âœ… All Silver tables created successfully
24 tests passed in 1.38s
```

---
```bash
# Make sure Docker is running
docker-compose ps

# If not, start it
docker-compose up -d
```

## ðŸ“š Key Files to Understand

| File | Purpose |
|------|---------|
| `src/ingestion/orders_loader.py` | CSV loading template |
| `src/ingestion/product_api_client.py` | REST API integration template |
| `src/ingestion/reviews_loader.py` | Review data loading template |
| `src/utils/logger.py` | Structured logging setup |
| `sql/ddl/star_schema.sql` | Dimensional model definition |
| `config/settings.yaml` | Pipeline configuration |
| `requirements.txt` | Python dependencies |
| `docker-compose.yml` | PostgreSQL + PgAdmin setup |

## ðŸŽ“ What You're Learning

âœ… **Data Engineering Skills**
- Loading from CSV and REST APIs
- Data validation and standardization
- Retry patterns and resilience
- Structured logging
- Unit testing

âœ… **Cloud Skills**
- Azure Storage (when you connect it)
- Docker and containerization
- Environment management

âœ… **Best Practices**
- Modular code structure
- Error handling
- Logging and monitoring
- Testing patterns

## ðŸš€ Ready to Move to Phase 2?

Once all tests pass and you can successfully run the ingestion modules:

1. All raw data loads successfully âœ…
2. Logging shows proper flow âœ…
3. Tests pass âœ…
4. Database is accessible âœ…

Then start Phase 2: **Unstructured Review Extraction**

Next you'll build:
- Sentiment analysis module (using TextBlob)
- Keyword extraction
- Combined review processing pipeline
- Tests for extraction logic

---

## Phase 4: Silver to Gold Dimensional Modeling + PostgreSQL (Using Polars)

**Status: ðŸŽ‰ COMPLETE**

Phase 4 transforms clean Silver layer data into a dimensional model (star schema) in the Gold layer using **Polars** for fast, efficient transformations. Then loads it into PostgreSQL for analytics.

### What Phase 4 Does

**Dimensional Modeling (Star Schema) with Polars:**
1. **Dimension Tables:**
   - `dim_product` â€” product attributes (name, category, price, stock)
   - `dim_customer` â€” customer location (state, city, zip)
   - `dim_date` â€” calendar attributes (year, month, day, weekday, etc.)

2. **Fact Table:**
   - `fact_orders` â€” one row per order with measures and quality flags

**Why Polars:**
- âœ“ **Fast** â€” 10x faster than pandas for large datasets
- âœ“ **Memory efficient** â€” lazy evaluation
- âœ“ **Modern** â€” built for data engineering (Apache Arrow backend)

### Phase 4 Quick Start

```bash
# 1. Ensure Phases 1-3 are complete
uv run python 
uv run python 
uv run python 
uv run python 
uv run python 

# 2. Run Gold layer pipeline (dimensional modeling)
uv run python 

# 3. Run tests
uv run pytest tests/unit/test_silver_to_gold.py -v
```

### Phase 4 Step-by-Step

#### Step 1: Verify Silver Tables Exist (1 minute)

```bash
ls C:\ecommerce_delta_lake\silver\
```

Expected: `orders/`, `reviews/`, `products/`, `orders_reviews/`

---

#### Step 2: Run Gold Layer Pipeline (2 minutes)

```bash
uv run python 
```

**Expected Output:**
```
============================================================
PHASE 4: SILVER â†’ GOLD TRANSFORMATION + POSTGRESQL LOAD
============================================================

ðŸ“Š TRANSFORMATION SUMMARY (Delta Lake)
------------------------------------------------------
  dim_product:        194 rows
  dim_customer:        XX rows
  dim_date:           ~45 rows
  fact_orders:     1,000 rows

â±ï¸  Transformation Duration: 1.50 seconds

ðŸ’¾ POSTGRESQL LOAD SUMMARY
------------------------------------------------------
  dim_product
    â”œâ”€ Inserted: 194
    â”œâ”€ Updated:   0
    â””â”€ Total:   194
  dim_customer
    â”œâ”€ Inserted:  XX
    â”œâ”€ Updated:   0
    â””â”€ Total:    XX
  dim_date
    â”œâ”€ Inserted:  ~45
    â”œâ”€ Updated:   0
    â””â”€ Total:    ~45
  fact_orders
    â”œâ”€ Inserted: 1,000
    â”œâ”€ Updated:   0
    â””â”€ Total:  1,000

âœ… PHASE 4 COMPLETE
============================================================
```

**What This Means:**
- âœ“ Silver tables read with Polars
- âœ“ Dimensional model built
- âœ“ All Gold Delta tables written
- âœ“ All data loaded to PostgreSQL with idempotent upserts

---

#### Step 3: Verify Gold Tables Created (1 minute)

```bash
ls C:\ecommerce_delta_lake\gold\
```

Expected: `fact_orders/`, `dim_product/`, `dim_customer/`, `dim_date/`

---

#### Step 4: Query PostgreSQL (5 minutes)

```bash
# Option A: PgAdmin web interface
# http://localhost:5050 â†’ admin@example.com / admin

# Option B: Command line (psql)
docker exec -it ecommerce-postgres psql -U postgres -d ecommerce_db

# Example query:
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
```

---

#### Step 5: Run Phase 4 Tests (1 minute)

```bash
uv run pytest tests/unit/test_silver_to_gold.py -v

# Expected: 16 tests passing
```

---

#### Step 6: Run All Tests (2 minutes)

```bash
uv run pytest tests/unit/ -v

# Expected: 40+ tests passing (Phases 1-4)
```

---

### Phase 4 Checklist

- Completed Build dim_product with Polars (unique products, surrogate keys)
- Completed Build dim_customer with Polars (unique customers, surrogate keys)
- Completed Build dim_date with Polars (calendar, YYYYMMDD IDs)
- Completed Build fact_orders with Polars (joins to dimensions, sentiment)
- Completed Write Gold Delta tables
- Completed Load all Gold tables to PostgreSQL (idempotent upserts)
- Completed Write 13 analyst-ready SQL queries
- Completed Pass all Phase 4 unit tests (16 tests)
- Completed Verify end-to-end transformation

### Phase 4 Key Modules

**src/transform/silver_to_gold.py** (Polars-based Transformation)
- `build_dim_product()` â€” Extract unique products
- `build_dim_customer()` â€” Extract unique customers
- `build_dim_date()` â€” Build calendar dimension
- `build_fact_orders()` â€” Join orders to dimensions
- `write_gold_table()` â€” Write Gold Delta tables
- `transform_silver_to_gold()` â€” Orchestrate complete transformation

**src/db/loader.py** (PostgreSQL Loading)
- `PostgreSQLLoader` â€” Context manager for DB connection
- `upsert_dim_product()` â€” Load product dimension
- `upsert_dim_customer()` â€” Load customer dimension
- `upsert_dim_date()` â€” Load date dimension
- `upsert_fact_orders()` â€” Load fact table
- `load_gold_to_postgresql()` â€” Orchestrate all loads

**src/db/queries.py** (Analyst SQL Queries)
- 13 production-ready queries for revenue, products, sentiment, trends
- Data quality audit queries

**src/transform/run_phase4.py** (Execution Script)
- User-friendly entry point
- Orchestrates transformation + PostgreSQL load

**tests/unit/test_silver_to_gold.py** (Unit Tests)
- 16 tests covering dimension building, fact table joins, error handling

### How to Check Phase 4

**Option 1: Run Tests** (fastest)
```bash
uv run pytest tests/unit/test_silver_to_gold.py -v
```

**Option 2: Query PostgreSQL**
```bash
docker exec -it ecommerce-postgres psql -U postgres -d ecommerce_db
\dt  # List all tables
SELECT COUNT(*) FROM fact_orders;
SELECT * FROM dim_product LIMIT 5;
```

**Option 3: Check Logs**
```bash
Get-Content .\logs\src_transform_silver_to_gold.log -Tail 20
```

---

## Phase 5: Orchestration with Prefect

**Status: Complete**

Phase 5 orchestrates all 4 phases into a single coordinated pipeline with daily scheduling, automatic retries, and error handling.

### Phase 5 Architecture

All phases run in sequence with error recovery:

```
Phase 1 (Ingestion)
    â†“ (on success)
Phase 2 (Sentiment Extraction)
    â†“ (on success)
Phase 3 (Bronze to Silver)
    â†“ (on success)
Phase 4 (Silver to Gold)
    â†“
Completion Summary
```

Each phase retries up to 2 times on failure with 60-second delays between retries.

### Phase 5 Quick Start

**Run the complete pipeline:**

```bash
uv run python 
```

**Expected output:**
```
======================================================================
ECOMMERCE DATA PIPELINE: ORCHESTRATION
======================================================================

Starting Phase 1: Ingestion
Phase 1 Complete: All sources ingested

Starting Phase 2: Sentiment Extraction
Phase 2 Complete: 800 reviews enriched with sentiment

Starting Phase 3: Bronze to Silver Transformation
Phase 3 Complete: Silver layer created

Starting Phase 4: Silver to Gold Transformation
Phase 4 Complete: Gold layer created
  - dim_product: 194 rows
  - dim_customer: XX rows
  - dim_date: ~45 rows
  - fact_orders: 1000 rows

======================================================================
COMPLETE PIPELINE SUCCESSFUL
======================================================================
Total Duration: XX.XX seconds
```

### Phase 5 With Prefect Scheduling

**Install Prefect (optional, for scheduling):**

```bash
pip install prefect>=2.0
```

**Run with Prefect scheduling:**

```bash
uv run python 
```

**Deploy to Prefect Cloud for daily scheduling:**

```bash
prefect cloud login
prefect deploy --name ecommerce-pipeline
```

### Phase 5 Features

- **Sequential Execution:** Phases run one after another
- **Automatic Retries:** Each phase retries 2 times on failure
- **Comprehensive Logging:** Full execution log at `./logs/`
- **Error Handling:** Catches and logs errors without stopping
- **Backfill Support:** Data partitioned by `_ingestion_date` for historical runs
- **Prefect Ready:** Can be deployed to Prefect Cloud for scheduling

### Phase 5 Key Files

**src/flows/ecommerce_pipeline.py**
- Orchestrates all 4 phases
- Runs standalone without Prefect
- 400+ lines of production-ready code

**src/flows/prefect_flow.py**
- Wraps pipeline with Prefect tasks
- Adds scheduling and retry logic
- Integrates with Prefect Cloud

### Phase 5 Checklist

- [x] Create orchestration pipeline
- [x] Implement Phase 1-4 task execution
- [x] Add retry logic (2 retries, 60s delay)
- [x] Add comprehensive error handling
- [x] Create Prefect flow wrapper
- [x] Add scheduling support (2 AM daily)
- [x] Add execution summary logging
- [x] Support backfill capability

### Next Steps (Phase 6)

Phase 6 will add:
- Unit tests for pipeline logic
- Integration tests with mock data
- Data quality assertions
- Monitoring dashboards
- Email/Slack notifications on failure

---

---


