# E-Commerce Sales & Customer Feedback Pipeline
### A simple, easy-to-explain, resume-ready Data Engineering project roadmap

---

## 1. The Business Problem (30-second explanation)

An online store's data lives in three disconnected places: **daily order exports** (sales), a **product catalog API** (what's being sold, prices, categories), and **customer reviews** (unstructured text feedback). Management wants one daily dashboard showing: revenue trends, best/worst-selling products, and which products are getting negative feedback — without anyone manually copy-pasting from three systems every morning.

**Your project:** an automated pipeline that ingests all three sources daily, cleans and joins them, stores them in a Bronze/Silver/Gold data lake, loads curated data into a database, and powers a sales + sentiment dashboard.

This is instantly understandable to any interviewer — no domain background needed — while still exercising every real data engineering skill.

---

## 2. Data Sources

| Source | Type | What it is |
|---|---|---|
| **Orders data** | CSV (batch file, simulating a daily export) | Use the free **Olist Brazilian E-Commerce** dataset (Kaggle, ~100k real orders) or generate synthetic daily order files with `Faker`. This is your "batch file" source. |
| **Product catalog** | REST API, JSON | **DummyJSON** or **FakeStore API** (free, no auth) — product name, price, category, stock. Simulates a live product-service API a real company would have. |
| **Customer reviews** | Unstructured text | Review text bundled in the Olist dataset (or scrape/simulate short review comments). You extract structured signals: sentiment (positive/negative), flagged keywords (e.g. "broken", "late", "refund") — this is your unstructured→structured showcase. |
| **Reference data** | CSV | Category mapping, region/state lookup |

Three genuinely different formats (batch CSV, API/JSON, unstructured text) — realistic and easy to describe.

---

## 3. Architecture

```
   ORDERS (CSV)      PRODUCT API (JSON)      REVIEWS (unstructured text)
        │                    │                          │
        └────────────────────┴──────────┬───────────────┘
                                          ▼
                          ┌─────────────────────────┐
                          │   BRONZE (raw landing)    │  Azure Blob Storage
                          │   as-is, partitioned by    │  /bronze/source/date/
                          │   source/date               │
                          └────────────┬─────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │  SILVER (cleaned, typed,  │  ADLS Gen2, Delta/Parquet
                          │  deduplicated, sentiment   │  /silver/entity/
                          │  extracted)                 │
                          └────────────┬─────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │  GOLD (star schema:        │  Delta + Azure SQL/Postgres
                          │  fact_orders + dims)        │
                          └────────────┬─────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   Dashboard (Streamlit /   │
                          │   Power BI)                 │
                          └─────────────────────────┘

   Cross-cutting: pandera/Great Expectations checks · logging · Prefect
   scheduling · GitHub Actions CI/CD · Docker · pytest
```

Same proven medallion pattern as any real company — just with data anyone can reason about.

---

## 4. Technology Choices & Why

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python (`requests`) | Direct, matches your existing skill |
| Transformation | Pandas (primary) + Polars for one step | You already use pandas daily; add one Polars step so you can speak to both in interviews |
| Storage | Azure Blob Storage → ADLS Gen2 | You already use this — deepen it into Bronze/Silver/Gold |
| Table format | **Delta Lake** (`deltalake` package) | You already use this at work — reuse deliberately, strong interview material |
| Orchestration | **Prefect** | You already use this professionally — no new tool to learn, more time for the parts that are actually new |
| Data quality | `polars` (built-in validation, fast, no dependencies) | Schema + range validation with minimal setup |
| Database | Postgres (Docker locally) → Azure SQL/PostgreSQL Flexible Server for cloud | SQL integration + realistic "serving layer" |
| Containerization | Docker + docker-compose | Package pipeline + Postgres for local dev |
| CI/CD | GitHub Actions | Standard, free |
| Dashboard | Streamlit | Fastest path to a working, demoable dashboard |
| Testing | pytest | Standard |
| Monitoring | Python `logging` + Prefect's run UI | Same discipline you already apply at work |

---

## 5. Project Folder Structure

```
ecommerce-pipeline/
├── .github/workflows/
│   ├── ci.yml
│   └── cd.yml
├── config/
│   └── settings.yaml
├── src/
│   ├── ingestion/
│   │   ├── orders_loader.py
│   │   ├── product_api_client.py
│   │   └── reviews_loader.py
│   ├── extraction/
│   │   └── review_sentiment.py       # unstructured -> structured
│   ├── transform/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   ├── quality/
│   │   └── checks.py
│   ├── storage/
│   │   └── adls_client.py
│   ├── db/
│   │   ├── models.py
│   │   └── loader.py
│   └── utils/logger.py
├── flows/
│   └── daily_pipeline_flow.py
├── tests/
│   ├── unit/
│   └── integration/
├── sql/
│   ├── ddl/
│   └── analysis/
├── dashboard/
│   └── app.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── docs/
│   ├── architecture.png
│   └── data_dictionary.md
├── .env.example
├── requirements.txt
└── README.md
```

---

## 6. Step-by-Step Implementation Plan

### Phase 0 — Setup (1-2 days)
**Learn:** medallion architecture, star schema basics.
**Do:** create repo (`main` + `develop` branches), set up Azure storage account with bronze/silver/gold containers, download the Olist dataset, sketch your star schema (`fact_orders` + `dim_product`, `dim_customer`, `dim_date`).

### Phase 1 — Ingestion (Week 1)
**Learn:** reading REST APIs, chunked CSV reading, retry patterns (`tenacity`).
**Do:** write `orders_loader.py` (read daily order CSV slice), `product_api_client.py` (pull product catalog from DummyJSON), `reviews_loader.py` (load review text). Land all three raw, untouched, into **Bronze**, partitioned by `source/date`. Add logging + retries.

### Phase 2 — Unstructured Review Extraction (Week 2)
**Learn:** basic sentiment analysis (`TextBlob` or `vaderSentiment` — both simple, no ML training needed), keyword flagging with regex.
**Do:** for each review, extract: `sentiment_score`, `sentiment_label` (positive/neutral/negative), `flagged_keywords` (e.g. "broken", "late", "refund", "defective"). Write unit tests covering edge cases (empty review, non-English text, all-caps rage review).

### Phase 3 — Bronze → Silver (Week 2-3)
**Learn:** Delta Lake basics, data typing/standardization.
**Do:** parse raw files into typed DataFrames, standardize currency/date formats, dedupe orders, join reviews to orders on order ID. Write to Silver as Delta tables. Add `pandera` checks: no negative prices, no null order IDs, valid date ranges.

### Phase 4 — Silver → Gold + SQL (Week 3)
**Learn:** dimensional modeling, upsert/MERGE patterns.
**Do:** build `fact_orders` (order line, price, sentiment) + dimension tables in Gold. Load into Postgres/Azure SQL with idempotent upserts. Write 5-8 analyst SQL queries (revenue by category, top negative-sentiment products, monthly trend).

### Phase 5 — Orchestration (Week 4)
**Learn:** Prefect deployments and scheduling (you know the fundamentals — apply them here).
**Do:** wire Phases 1-4 into one Prefect flow, schedule it daily, add retries + failure alert (email/Slack webhook), test a backfill for a past date range.

### Phase 6 — Testing, Quality, Monitoring (Week 4-5)
**Learn:** pytest mocking for API calls.
**Do:** unit tests for extraction/transform logic (mocked, no live network calls), one integration test running the full flow on a small fixture dataset, data quality summary logged per run.

### Phase 7 — Docker + CI/CD (Week 5)
**Learn:** Dockerfile basics, GitHub Actions secrets.
**Do:** `Dockerfile` + `docker-compose.yml` (pipeline + Postgres for local dev). `ci.yml`: lint + unit tests on every PR. `cd.yml`: build/push image to Azure Container Registry on merge to `main`.

### Phase 8 — Dashboard & Polish (Week 5-6)
**Learn:** Streamlit basics.
**Do:** build a dashboard — revenue trend, top products, sentiment breakdown, list of today's flagged negative reviews. Write README + data dictionary.

---

## 7. Git/GitHub Workflow

- `main` (deployable) + `develop` (integration) + `feature/<phase>` branches.
- One PR per phase/task, even solo — real PR descriptions become interview evidence of process.
- Conventional commits (`feat:`, `fix:`, `test:`, `docs:`); tag releases per phase (`v0.1`...`v0.8`).

---

## 8. Testing Strategy

- **Unit tests**: pure functions — parsers, sentiment extraction, validators — mocked inputs, no network.
- **Integration test**: full flow on a small fixture dataset checked into `tests/fixtures/`.
- **Data quality**: `pandera` checks run as part of the flow itself, not just CI.

---

## 9. Resume Bullet Points (write after completing)

- Built an end-to-end ETL pipeline ingesting order (CSV), product (REST API), and unstructured customer review data into a Bronze/Silver/Gold data lake on Azure ADLS Gen2 using Delta Lake.
- Developed unstructured-to-structured extraction logic to derive sentiment and flagged issues from customer reviews, surfacing negative-feedback trends automatically.
- Orchestrated a daily pipeline using Prefect with retries, scheduling, and failure alerting; containerized with Docker and deployed via GitHub Actions CI/CD.
- Implemented automated data quality validation (`pandera`) across schema, null, and range checks at each pipeline stage.
- Designed a star-schema Gold layer in PostgreSQL/Azure SQL and built a Streamlit dashboard for daily sales and sentiment insights.

---

## 10. README Structure

1. Project overview (the one-sentence pitch)
2. Architecture diagram
3. Tech stack table
4. Demo screenshot/GIF
5. How it works (Bronze → Silver → Gold walkthrough)
6. Setup instructions (Docker Compose)
7. Data quality approach
8. CI/CD badge + explanation
9. What I'd change at scale
10. License/contact

---

## 11. Likely Interview Questions This Prepares You For

- Walk me through what happens to one order from raw CSV to dashboard.
- How do you handle a malformed row in the daily order file — does the whole pipeline fail?
- Why Bronze/Silver/Gold instead of one staging table?
- How did you turn free-text reviews into something queryable, and how do you know it's accurate?
- How do you make the pipeline safe to re-run (idempotency)?
- Why Delta Lake over plain Parquet here?
- How would you scale this from one store's data to 500 stores?
- What would you monitor in production and how would you get alerted?
- Why Prefect over Airflow — what's the actual trade-off?

---

## Suggested Pace

**5-6 weeks** at a few hours/day. Commit from Phase 0 onward so your GitHub history shows iterative, real progress — that's often more convincing to interviewers than the finished product alone.

Start with **Phase 0 + Phase 1** — get raw orders, product data, and reviews landing in Bronze — and we'll pick up from there.
