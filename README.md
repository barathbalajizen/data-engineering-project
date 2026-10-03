# End-to-End E-Commerce Data Pipeline

[![ci](https://github.com/barathbalajizen/data-engineering-project/actions/workflows/ci.yml/badge.svg)](https://github.com/barathbalajizen/data-engineering-project/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD4)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B)
![Prefect](https://img.shields.io/badge/Prefect-3-024DFD)

A batch data platform for an online store, built end to end. It **extracts** orders incrementally from an operational Postgres database, **cleans** them in a Delta Lake (Bronze → Silver), **models** a star schema with history tracking in dbt (Gold), **checks** data quality, and is **orchestrated, scheduled and monitored** with Prefect. A Streamlit dashboard shows the results. Everything runs locally with one Docker command and costs nothing.

- **Live dashboard:** _add your Streamlit Community Cloud link here_
- **Sample output (no setup needed):** [docs/sample_output/](docs/sample_output/README.md)

---

## Highlights

- **Incremental loading** with a watermark and an overlap window: only new or changed rows are read, and late rows are not missed.
- **Idempotent at every layer** (Delta `MERGE`, truncate-and-reload, dbt rebuild): reruns and retries never create duplicates.
- **Medallion architecture** on Delta Lake: raw Bronze, cleaned and validated Silver, modelled Gold.
- **Star schema + SCD Type 2** in dbt: customer history is kept, and each order joins the customer version valid at purchase time.
- **Data quality**: invalid rows are quarantined with a reason (never silently dropped), and reconciliation checks run after every load and are logged to `audit.dq_log`.
- **Orchestration with Prefect**: daily schedule, retries with backoff, failure alerts, run summaries, parameterised backfills, no overlapping runs.
- **Recovery**: a chunked backfill rebuilds any lost date range without touching the daily watermark.
- **Performance**: a data-skew demo comparing a naive join with broadcast, salting and Spark AQE.
- **Tested**: 17 pytest unit tests plus dbt tests, run by GitHub Actions on every push and pull request.

---

## Architecture

```
 ┌──────────────────┐   incremental extract     ┌──────────────────────┐
 │ Postgres source  │ ── (watermark + overlap) ─▶│ BRONZE  (Delta Lake) │  raw rows + ingestion metadata
 │ (simulated OLTP) │                            └──────────┬───────────┘
 └──────────────────┘                                       │ trim, dedupe, validate, MERGE
                                                            ▼
                         invalid rows ◀── quarantine ┌──────────────────────┐
                                                     │ SILVER  (Delta Lake) │  clean, one row per key
                                                     └──────────┬───────────┘
                                                                │ Spark JDBC
                                                                ▼
                                                     ┌──────────────────────┐
                                                     │ Postgres `staging`   │
                                                     └──────────┬───────────┘
                                                                │ dbt build (models + tests + SCD2 snapshot)
                                                                ▼
                                                     ┌──────────────────────┐
                                                     │ GOLD `analytics`     │──▶ Streamlit dashboard
                                                     │ star schema + aggs   │
                                                     └──────────┬───────────┘
                                                                ▼
                                                     quality checks → audit.dq_log

 Prefect server + UI (:4200)  ◀──  pipeline container runs every step on schedule or on demand
```

---

## Tech stack

| Area | Tools |
|---|---|
| Processing | PySpark 3.5 (local mode), Delta Lake 3.2 |
| Storage / warehouse | PostgreSQL 15 |
| Transformation & modelling | dbt-core 1.8 (dbt-postgres) |
| Orchestration | Prefect 3 |
| Dashboard | Streamlit |
| Testing & CI | pytest, dbt tests, GitHub Actions |
| Infrastructure | Docker, Docker Compose |

---

## What I built: the pipeline step by step

### 1. Source system
[`generate_sample_data.py`](src/generate_sample_data.py) creates realistic e-commerce data (customers, orders, items, payments, products, sellers) in the format of the public [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). [`load_source.py`](src/load_source.py) loads it into a Postgres `source` schema that acts as the company's operational database. [`simulate_changes.py`](src/simulate_changes.py) simulates a day of activity (orders delivered, customers moving city, new orders) to show incremental loading and history tracking.

### 2. Bronze: incremental extraction
[`extract_bronze.py`](src/extract_bronze.py) reads only orders changed since the last run, using a **watermark** stored in `control.watermark`. Each run re-reads a 10-minute **overlap** to catch late-committed rows. Data is written to Delta Lake with an **insert-if-not-exists MERGE** on `(order_id, updated_at)`, so a rerun never inserts a row twice. The watermark moves forward only after a successful write.

Every Bronze row carries `ingestion_ts`, `batch_id` (unique per table per run), `source_system`, `pipeline_run_id` and `source_table`. Each table load is recorded in `audit.pipeline_run_log` (rows read/inserted, duration, status, retries) and `audit.lineage` (source → target, with the Delta version written).

#### Schema drift
Before writing, [`schema_drift.py`](src/schema_drift.py) compares the source schema with the schema already accepted in Bronze:

| Change | Action |
|---|---|
| New column | Accepted: Bronze, Silver and staging evolve (older rows get `NULL`) |
| Narrower type (e.g. `int` into a `bigint` column) | Accepted: cast to the existing type |
| Removed column | Rejected: the table fails, Bronze is not written. Set `SCHEMA_DRIFT_ALLOW_REMOVED_COLUMNS=true` to fill it with `NULL` instead |
| Any other type change | Rejected |

Every change and its action is recorded in `audit.schema_changes`. To accept a rejected change on purpose, change the Bronze table explicitly (for example rewrite it with the new schema) and rerun. Without this check, Delta MERGE would silently drop new source columns.

#### Time travel and Change Data Feed
Bronze tables keep 30 days of history (`BRONZE_LOG_RETENTION`). `bronze/orders` has **Change Data Feed** enabled, so downstream steps can read only the rows that changed. The `ecommerce-delta-inspect/run` deployment (read-only) shows a table's `history`, compares an old version with today (`as-of`), or lists the `changes` between versions.

### 3. Silver: cleaning and validation
[`transform_silver.py`](src/transform_silver.py) (helpers in [`transforms.py`](src/transforms.py)) trims strings, removes duplicates (keeping the latest version) and validates rows. **Invalid rows go to a quarantine table with a reason** instead of being dropped. Orders are upserted with a Delta MERGE where only newer versions win.

### 4. Load to the warehouse
[`load_warehouse.py`](src/load_warehouse.py) loads Silver into Postgres `staging` with Spark JDBC (truncate and reload, so it is safe to rerun).

### 5. Gold: dimensional model with dbt
The [dbt project](dbt_project/) builds a **star schema** in the `analytics` schema:

| Model | Description |
|---|---|
| `fact_orders` | One row per order item: price, freight, delivery days, late flag |
| `fact_payments` | One row per order payment |
| `dim_customer` | **SCD Type 2** from a dbt snapshot (`valid_from`, `valid_to`, `is_current`) |
| `dim_product`, `dim_seller`, `dim_date` | Dimensions with surrogate keys |
| `agg_daily_sales`, `agg_category_revenue` | Reporting aggregates for the dashboard |

`fact_orders` uses a **point-in-time join**: each order links to the customer version that was valid when the order was placed. dbt tests check unique/not-null keys and fact-to-dimension relationships.

### 6. Data quality checks
[`checks.py`](src/checks.py) runs after every load and writes results to `audit.dq_log`: no duplicate versions in Bronze, row-count reconciliation (source vs Silver vs staging), unique keys, no missing customer keys in the fact table, and data freshness. A critical failure fails the pipeline.

### 7. Orchestration with Prefect
[`flows/`](flows/) wraps every step as a Prefect task:
- **Daily schedule** (cron, configurable time zone) for the incremental load.
- **Retries**: 3 attempts with backoff (1, 2, then 4 minutes). A failed dbt build resumes with `dbt retry` from the failed model.
- **Alerts**: failure and crash hooks post to a Slack-compatible webhook (`ALERT_WEBHOOK_URL`).
- **Monitoring**: full logs, task timeline, and a run-summary artifact (row counts, watermark, quality results) for each run.
- **No overlapping runs** (`limit=1`), so a backfill and the daily run never collide.

### 8. Backfill and recovery
The backfill flow re-extracts any `[start, end)` date range in chunks (each chunk retries on its own), then rebuilds downstream, without moving the daily watermark. A "Bronze loss" simulation deletes a date range, so recovery can be shown end to end.

### 9. Performance: data skew
[`skew_demo.py`](src/skew_demo.py) runs the same join on a heavily skewed key four ways (naive sort-merge, broadcast, salting, Spark AQE) and prints the comparison.

### 10. Dashboard and published results
[`dashboard/app.py`](dashboard/app.py) is a Streamlit app: KPIs, monthly and daily sales, revenue by category, quality-check results, rows per layer and customer history. [`export_showcase.py`](src/export_showcase.py) exports a snapshot of the results to [docs/sample_output/](docs/sample_output/README.md), so the results are visible on GitHub and the dashboard runs on Streamlit Cloud without a database.

---

## How to run

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with **6 GB+ memory** (Settings → Resources)
- About 10 GB free disk space, and internet for the first build

### Step 1: Clone and start
```bash
git clone https://github.com/barathbalajizen/data-engineering-project.git
cd data-engineering-project
docker compose up -d --build
```
The first build takes 10–15 minutes (Java, Spark, Delta, Prefect, dbt). It starts three services: `postgres`, `prefect-server` and `pipeline`. On Linux, if the containers cannot write to `lake/`, run `chmod -R 777 lake` once.

### Step 2: Open Prefect
Go to **http://localhost:4200** → **Deployments**. To run one, click it, then **Run → Quick run** (or **Custom run** to change parameters).

### Step 3: Load data and run the full pipeline
Run **`ecommerce-setup/run`**. It generates the data, loads the source database and runs the whole pipeline once (a few minutes). Open the run to see the task timeline and logs, and the **Artifacts** tab for the run summary.

> To use the real Olist dataset instead of generated data, put its six CSV files (`olist_customers_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`) in `data/raw/` and run `ecommerce-setup/run` with `generate_csvs = false`.

### Step 4: Open the dashboard
```bash
docker compose --profile dashboard up -d --build
```
Go to **http://localhost:8501**.

### Step 5 (optional): Publish the results
Run **`ecommerce-export-showcase/run`**, then commit and push `docs/sample_output/`. For a public dashboard link, create an app on [share.streamlit.io](https://share.streamlit.io) from this repo with main file `dashboard/app.py`.

### All deployments

| Deployment | What it does |
|---|---|
| `ecommerce-setup/run` | First-time setup: generate data, load the source, run the pipeline. Parameters: `n_orders` (20000), `generate_csvs`, `run_pipeline_after` |
| `ecommerce-daily/daily` | Incremental load: Bronze → Silver → staging → dbt → checks. Scheduled daily at 02:00 (`DAILY_CRON`, `SCHEDULE_TZ`) |
| `ecommerce-backfill/backfill` | Reprocess orders in `[start, end)`. Parameters: `start`, `end`, `chunk_days` (31), `rebuild_downstream` |
| `ecommerce-simulate-changes/run` | Simulate a day of changes: 200 orders delivered, 100 customers move, 500 new orders |
| `ecommerce-bronze-health/run` | Row counts per layer and the number of duplicate versions (should be 0) |
| `ecommerce-simulate-bronze-loss/run` | Delete a date range from Bronze to practise recovery. Parameters: `start`, `end` |
| `ecommerce-skew-demo/run` | Data-skew comparison. Parameters: `rows`, `hot_share`, `salts` |
| `ecommerce-export-showcase/run` | Export a result snapshot to `docs/sample_output/` |
| `ecommerce-delta-inspect/run` | Read-only Delta history, time travel (`as-of`) and Change Data Feed (`changes`). Parameters: `action`, `table` (e.g. `bronze/orders`), `version`, `timestamp`, `from_version` |

---

## Demo scenarios

After Step 3, these show the main features:

| Feature | What to do | What you should see |
|---|---|---|
| **Incremental load + SCD2** | Run `ecommerce-simulate-changes/run`, then `ecommerce-daily/daily` | Only about 700 changed rows are extracted; 100 customers get a second row in `analytics.dim_customer` |
| **Idempotency** | Run `ecommerce-daily/daily` again, then `ecommerce-bronze-health/run` | 0 new versions inserted, `DUPLICATE versions=0` |
| **Backfill / recovery** | Run `ecommerce-simulate-bronze-loss/run` with `start=2017-03-01`, `end=2017-04-01`, then `ecommerce-backfill/backfill` with the same dates | The health check shows rows missing, then restored; the watermark is unchanged |
| **Retries** | Start `ecommerce-daily/daily`, run `docker compose stop postgres` during `extract-bronze`, then `docker compose start postgres` | The task goes to *AwaitingRetry* and succeeds on the next attempt |
| **Data skew** | Run `ecommerce-skew-demo/run` | A timing comparison of naive, broadcast, salted and AQE joins in the task log |

Query the warehouse directly at `localhost:5433` (user `de`, password `de`, database `shop`):
```sql
-- SCD Type 2: customers with more than one version
SELECT customer_id, customer_city, valid_from, valid_to, is_current
FROM analytics.dim_customer
WHERE customer_id IN (SELECT customer_id FROM analytics.dim_customer GROUP BY 1 HAVING count(*) > 1)
ORDER BY customer_id, valid_from LIMIT 10;

-- Latest data quality results
SELECT * FROM audit.dq_log ORDER BY id DESC LIMIT 10;
```

---

## Testing

The project has **17 pytest unit tests** for the core logic and **dbt tests** on the Gold models. GitHub Actions runs pytest and `dbt parse` on every push and pull request.

| Test file | What it checks |
|---|---|
| [`test_transforms.py`](tests/test_transforms.py) | Deduplication keeps the latest version; valid/invalid split for quarantine; string trimming |
| [`test_windowing.py`](tests/test_windowing.py) | Extract window: watermark overlap, first run, backfill windows ignore the watermark, invalid ranges rejected |
| [`test_split_window.py`](tests/test_split_window.py) | Backfill chunking: consecutive chunks with no gaps, last chunk clipped, bad inputs rejected |
| [`test_resilience.py`](tests/test_resilience.py) | Retry helper succeeds after transient failures and gives up after the limit |
| [`test_skew.py`](tests/test_skew.py) | Salted join gives the same result as a plain join and spreads the hot key; hot-key detection |

### Run the tests in Docker (easiest)
With the stack running:
```bash
docker compose exec pipeline pytest tests -q                       # all tests, short output
docker compose exec pipeline pytest tests -v                       # all tests, one line per test
docker compose exec pipeline pytest tests/test_transforms.py -v    # one file
docker compose exec pipeline pytest tests/test_windowing.py::test_backfill_window_ignores_watermark -v   # one test
docker compose exec pipeline pytest tests -k skew -v               # tests whose name contains "skew"
```

### Run the tests without Docker
Needs **Python 3.11** and **Java 17** (the same as CI):
```bash
pip install pyspark==3.5.1 pytest==8.2.2
pytest tests -v
```

### dbt tests
```bash
# Run the dbt tests against the warehouse (stack running, after Step 3)
docker compose exec pipeline bash -c "cd dbt_project && /opt/dbt_venv/bin/dbt test"

# Validate the dbt project without a database (what CI does)
pip install -r requirements-dbt.txt
DBT_PROFILES_DIR=dbt_project dbt parse --project-dir dbt_project
```

---

## Project structure

```
├── flows/                   Prefect flows and deployments
│   ├── ecommerce_flows.py     daily + backfill flows, retries, alerts, run summary
│   ├── ops_flows.py           setup, simulation, health, skew and export flows
│   └── serve.py               registers all deployments and the daily schedule
├── src/                     pipeline scripts (each also runs on its own)
│   ├── extract_bronze.py      incremental extract → Bronze
│   ├── transform_silver.py    clean, validate, quarantine, MERGE → Silver
│   ├── load_warehouse.py      Silver → Postgres staging
│   ├── checks.py              data quality and reconciliation checks
│   ├── export_showcase.py     export results to docs/sample_output
│   └── ...                    data generation, simulations, helpers
├── dbt_project/             Gold star schema, SCD2 snapshot, dbt tests
├── dashboard/               Streamlit dashboard
├── tests/                   pytest unit tests
├── sql/init.sql             schemas, watermark table, audit log
├── docs/sample_output/      exported results (visible on GitHub)
├── docker-compose.yml       Postgres, Prefect, pipeline runner, dashboard
└── .github/workflows/       CI: pytest + dbt parse
```

---

## Design decisions and trade-offs

- **Watermark + MERGE**: cheap incremental reads and safe reruns. Limitation: a row changed without updating `updated_at`, or a deleted row, is not detected; in production I would use CDC (for example Debezium).
- **Quarantine instead of dropping**: bad data stays visible and countable, so it can be fixed at the source.
- **SCD2 with a point-in-time join**: reports show the customer's city *at the time of the order*, not today's city.
- **Prefect tasks run scripts in subprocesses**: each Spark job gets a clean JVM, and the scripts still run without Prefect (`src/run_pipeline.sh`).
- **Scaling path**: local Spark → Databricks/EMR, local folders → S3/ADLS, Postgres → Snowflake/BigQuery, Prefect `serve` → work pools on Docker/Kubernetes. The flow code stays the same.
- **Simplified on purpose**: a single environment, credentials in compose environment variables (a secrets manager in production), Prefect on SQLite, and Silver recomputed from all of Bronze each run.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Build fails while downloading | Check your internet and run `docker compose build` again |
| Deployments missing in Prefect | Check `docker compose logs pipeline`, then `docker compose restart pipeline` |
| Run stuck in *Late* / *Scheduled* | Another run is in progress (one at a time), or the `pipeline` container is down |
| Run fails with missing CSVs | Run `ecommerce-setup/run` first |
| A task failed | Open the run in Prefect and read the task log; fix the cause and click **Retry**. All steps are safe to rerun |
| Dashboard says "No data to show" | Run `ecommerce-setup/run`, then refresh the page |
| Spark out of memory | Give Docker 8 GB, or use a smaller `n_orders` / `rows` |
| Port 5433 / 4200 / 8501 in use | Change the left side of the port mapping in `docker-compose.yml` |
| `init.sql` changes not applied | It only runs on a fresh volume: `docker compose down -v` |

Useful commands:
```bash
docker compose logs -f pipeline          # follow the runner logs
docker compose down                      # stop (data is kept)
docker compose down -v                   # stop and delete all database data
```
