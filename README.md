# E-Commerce Analytics Platform (100% free, runs locally)

[![ci](https://github.com/barathbalajizen/data-engineering-project/actions/workflows/ci.yml/badge.svg)](https://github.com/barathbalajizen/data-engineering-project/actions/workflows/ci.yml)

Medallion-architecture pipeline: **Postgres (source) -> Delta Lake Bronze -> Silver -> Postgres staging -> dbt Gold (star schema + SCD2)**.
You start the stack with **one command** and then run, schedule, monitor and backfill everything from the **Prefect UI**.

```
Postgres source --(incremental, watermark)--> BRONZE (Delta, raw + metadata)
                                                 |  dedupe, validate, quarantine, MERGE
                                               SILVER (Delta)
                                                 |  Spark JDBC
                                           Postgres `staging`
                                                 |  dbt build (models + tests + SCD2 snapshot)
                                           Postgres `analytics` (GOLD)
                                                 |
                                           audit.dq_log (quality checks)

Prefect server + UI (:4200)  <--  pipeline container (flows/serve.py) executes the runs
```

## See the results without running anything
Browse **[docs/sample_output/](docs/sample_output/README.md)**: row counts per layer, monthly and daily sales, revenue per category, SCD Type 2 customer history and the data-quality results of the latest run, exported from a real run of this pipeline.

## Tech stack
| Layer | Tool |
|---|---|
| Source (OLTP) and warehouse | PostgreSQL 15 |
| Lake (Bronze / Silver) | Delta Lake 3.2 on PySpark 3.5 (local mode, Java 17) |
| Gold modelling | dbt-core 1.8 + dbt-postgres (separate virtualenv) |
| Orchestration | Prefect 3 (server + UI, `serve()` runner) |
| Dashboard (optional) | Metabase |
| CI | GitHub Actions: pytest + `dbt parse` |

## Requirements
- Docker Desktop (give it **6 GB+ RAM**: Settings > Resources)
- ~10 GB free disk, internet for the first build only

## Start (one command)

```bash
git clone https://github.com/barathbalajizen/data-engineering-project.git
cd data-engineering-project
docker compose up -d --build        # first build takes 10-15 min (Java, Spark, Delta, Prefect, dbt)
```
This starts only three services: **postgres**, **prefect-server** and **pipeline** (the runner that executes your flow runs). Then open **http://localhost:4200** and do everything from there. On Linux, if the containers cannot write to `lake/`, run `chmod -R 777 lake` once.

## Run it from the Prefect UI

Open **Deployments**. Each row below is a deployment; click it, then **Run > Custom run** (change parameters) or **Quick run**.

| Deployment | What it does | Parameters |
|---|---|---|
| `ecommerce-setup/run` | **Do this first.** Generates sample CSVs (or uses your own Olist CSVs in `data/raw`), loads the Postgres source, then runs the whole pipeline once. Replaces the source tables, so use it for first setup or a clean restart. | `n_orders` (20000), `generate_csvs`, `run_pipeline_after` |
| `ecommerce-daily/daily` | The incremental load: Bronze -> Silver -> staging -> dbt -> quality checks. **Scheduled** 02:00 (`SCHEDULE_TZ`, default `Asia/Kolkata`). Run it manually any time. | none |
| `ecommerce-backfill/backfill` | Re-extract orders with `updated_at` in `[start, end)`, then rebuild downstream. | `start`, `end`, `chunk_days` (31), `rebuild_downstream` |
| `ecommerce-simulate-changes/run` | Simulate a day of source activity: 200 orders delivered, 100 customers move, 500 new orders. | none |
| `ecommerce-bronze-health/run` | Logs Bronze/Silver/quarantine counts and the number of duplicate order versions (should be 0). | none |
| `ecommerce-simulate-bronze-loss/run` | Deletes a window from Bronze, so you can practise recovery with the backfill. | `start`, `end` |
| `ecommerce-export-showcase/run` | Exports a snapshot of the Gold results to `docs/sample_output/` (CSVs + `README.md`). Commit that folder to publish the results on GitHub. | none |
| `ecommerce-skew-demo/run` | Runs the same join naive, broadcast, salted and with AQE; the comparison table is in the task log. | `rows` (1,000,000), `hot_share`, `salts` |

### Suggested first session
1. Run `ecommerce-setup/run` (defaults). Watch the run's task timeline and logs; open **Artifacts** for the run summary (row counts, watermark, data-quality results).
2. Run `ecommerce-simulate-changes/run`, then `ecommerce-daily/daily`. The `extract-bronze` log shows only the changed rows (about 700) being picked up, and 100 customers get a second row in `analytics.dim_customer` (SCD Type 2).
3. Run `ecommerce-daily/daily` again: it inserts 0 new versions. Check with `ecommerce-bronze-health/run` (`DUPLICATE versions=0`). That is idempotency.
4. Run `ecommerce-bronze-health/run`, then `ecommerce-simulate-bronze-loss/run` with `start=2017-03-01`, `end=2017-04-01`, then health again (fewer rows), then `ecommerce-backfill/backfill` with the same dates, then health again (restored). The orders watermark is untouched by the backfill.
5. Run `ecommerce-export-showcase/run`, then commit `docs/sample_output/` so the results show on GitHub.
6. Practise failure handling: start `ecommerce-daily/daily`, then run `docker compose stop postgres` in a terminal while `extract-bronze` is running. The task goes to **AwaitingRetry** and retries after 1, 2, then 4 minutes. Run `docker compose start postgres` before the next attempt and it succeeds.

### Using the real Olist dataset (optional)
By default the setup flow generates synthetic data with the same file names and columns as the [Kaggle Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). To use the real data instead, put these files in `data/raw/` (they are git-ignored) and run `ecommerce-setup/run` with `generate_csvs=false`:

`olist_customers_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`

## Gold data model (`analytics` schema)

| Model | Grain / content |
|---|---|
| `fact_orders` | One row per order item: price, freight, `delivery_days`, `is_late`. Joined to the customer version valid at purchase time (point-in-time SCD2). |
| `fact_payments` | One row per order payment (`payment_sequential`): type and value |
| `dim_customer` | SCD Type 2 from the `customers_snapshot` dbt snapshot (`valid_from`, `valid_to`, `is_current`) |
| `dim_product`, `dim_seller` | One row per product / seller (surrogate key = `md5` of the natural key) |
| `dim_date` | Calendar 2016-2030, `date_key` = `YYYYMMDD` |
| `agg_daily_sales` | Orders, items, revenue, freight, late deliveries per day (canceled orders excluded) |
| `agg_category_revenue` | Orders, revenue and average delivery days per product category |

dbt tests (in `models/marts/schema.yml`) check unique/not-null keys and the fact-to-dimension relationships; they run as part of every `dbt build`.

## How Prefect is used

| Need | How |
|---|---|
| **Scheduling** | `flows/serve.py` registers `daily` with a cron schedule (`DAILY_CRON`, default `0 2 * * *`, in `SCHEDULE_TZ`; set in `docker-compose.yml`). Pause or edit it on the deployment page. |
| **Monitoring** | Dashboard (run history, failures), per-run task timeline, retry history, full logs (every script's output is streamed into the run), and a **run-summary artifact** per run under **Artifacts**. |
| **Alerting** | Failure and crash hooks log an error and post to `ALERT_WEBHOOK_URL` (Slack-compatible webhook) if set. Prefect **Automations** can add more (notify on failure, late or long-running runs). |
| **Retries** | Each pipeline task retries 3 times, waiting 1, 2, then 4 minutes. The quality-checks task does not retry (bad data is not transient). On a retry, `dbt_build.sh` runs `dbt retry` and resumes from the failed model. |
| **Backfill** | The window is split into chunks of `chunk_days`; each chunk is its own task with its own retries. Bronze inserts only versions it lacks and the daily watermark is untouched, so rerunning is safe. |
| **No overlapping runs** | `serve(..., limit=1)`: one flow run at a time. A backfill started at 02:00 makes the daily run wait, then start. |

Each task runs the existing script in `src/` in a subprocess. The scripts still work on their own, and `src/run_pipeline.sh` runs the whole pipeline without Prefect.

## Optional extras

```bash
docker compose --profile dashboard up -d      # also start Metabase on http://localhost:3000
docker compose logs -f pipeline               # runner logs (deployment registration, run start/finish)
docker compose exec pipeline pytest tests -q  # unit tests
docker compose exec pipeline bash src/run_pipeline.sh   # whole pipeline without Prefect (debugging)
docker compose exec pipeline prefect deployment run 'ecommerce-daily/daily'   # CLI alternative to the UI
docker compose down                           # stop (data kept)
docker compose down -v                        # stop and delete Postgres + Prefect data
rm -rf lake/bronze lake/silver                # (Windows: delete the folders) clear the lake
```
Metabase: add a PostgreSQL database with host `postgres`, port `5432`, db `shop`, user `de`, password `de`, then chart `analytics.agg_daily_sales`, `analytics.agg_category_revenue`, `analytics.fact_orders`. From your machine, Postgres is on port **5433** (`de`/`de`/`shop`):
```sql
SELECT customer_id, customer_city, valid_from, valid_to, is_current
FROM analytics.dim_customer
WHERE customer_id IN (SELECT customer_id FROM analytics.dim_customer GROUP BY 1 HAVING count(*) > 1)
ORDER BY customer_id, valid_from LIMIT 10;
SELECT * FROM audit.dq_log ORDER BY id DESC LIMIT 10;
```

## Project layout
| Path | Purpose |
|---|---|
| `docker-compose.yml`, `Dockerfile.pipeline` | Postgres, Prefect server, pipeline runner (+ optional Metabase). Spark/Delta/Prefect in one virtualenv, dbt in its own |
| `flows/ecommerce_flows.py` | Pipeline tasks, daily + backfill flows, failure hooks, run-summary artifact |
| `flows/ops_flows.py` | Setup, simulation, health and skew-demo flows (run from the UI) |
| `flows/serve.py` | Registers every deployment (schedule, tags) and runs them (`limit=1`) |
| `flows/trigger_backfill.py` | Optional: start a backfill from the command line |
| `sql/init.sql` | Schemas, watermark control table, `audit.dq_log` |
| `src/extract_bronze.py` | Incremental (orders) + full extracts -> Delta Bronze; `--start/--end` for backfill |
| `src/transform_silver.py`, `transforms.py` | Trim, dedupe, validate, quarantine, MERGE, skew helpers |
| `src/load_warehouse.py` | Silver -> Postgres staging (Spark JDBC) |
| `dbt_project/` | Gold star schema, SCD2 snapshot, tests |
| `src/checks.py` | Reconciliation + quality checks -> audit log |
| `src/windowing.py`, `src/resilience.py` | Extract window, backfill chunking, retry helper (pure Python, unit tested) |
| `src/dbt_build.sh`, `src/run_pipeline.sh` | dbt wrapper (`dbt retry` on later attempts); whole pipeline without Prefect |
| `src/generate_sample_data.py`, `load_source.py`, `simulate_changes.py` | Sample data and the simulated OLTP source |
| `src/bronze_stats.py`, `simulate_bronze_loss.py`, `skew_demo.py` | Demo helpers (wrapped by the ops flows) |
| `src/export_showcase.py`, `docs/sample_output/` | Exports a result snapshot (CSV + Markdown) that is committed, so the repo shows real output |
| `tests/`, `.github/workflows/ci.yml` | pytest unit tests; CI runs them and `dbt parse` on every push and pull request |

## Running the tests without Docker
The unit tests cover the pure-Python and Spark transform logic and do not need Postgres. You need Python 3.11 and Java 17 (same as CI):

```bash
pip install pyspark==3.5.1 pytest==8.2.2
pytest tests -q

pip install -r requirements-dbt.txt                       # optional: validate the dbt project
DBT_PROFILES_DIR=dbt_project dbt parse --project-dir dbt_project
```

## Idempotency, retries and backfill

| Concern | How it is handled |
|---|---|
| Rerun / crash after the Bronze write but before the watermark update | Bronze `orders` is written with an **insert-if-not-exists MERGE** on `(order_id, updated_at)`; the rerun inserts 0 rows. |
| Late-committed rows near the watermark | **Overlap window**: each run re-reads from `watermark - 10 min` (`WATERMARK_LOOKBACK_MINUTES`); the MERGE absorbs the repeats. The watermark never moves backwards. |
| Silver / Gold reruns | Silver `orders` MERGE (newer `updated_at` wins), other tables overwritten, staging truncated and reloaded, dbt rebuilds. Quarantine is rewritten each run (no pile-up). |
| Transient failures | Prefect task retries with backoff, plus `retry()` around database calls inside scripts. |
| dbt failure mid-build | On a retry, `dbt_build.sh` runs `dbt retry` and resumes from the failed node. |
| Missed schedule (runner was down) | Runs that were due stay scheduled and start once the runner is back. Loads are watermark-based and idempotent, so repeated catch-up runs are cheap and safe. |

## Runbook (what to do when a task fails)
Open the failed flow run in the UI, then the failed task's logs (script output and the last lines of the error).

| Failing task | Likely cause | Action |
|---|---|---|
| `extract-bronze` | Source down / DB credentials | Check Postgres is healthy; rerun. The watermark moves only after a successful write. |
| `transform-silver` | Schema change, out of memory | Read the log; raise `SPARK_DRIVER_MEMORY`; MERGE is idempotent, so rerun. |
| `load-warehouse` | Postgres connection | Rerun (table is truncated and reloaded). |
| `dbt-build` | A dbt test failed | The log names the test. Inspect the rows, fix upstream, rerun. |
| `quality-checks` | Reconciliation mismatch | Open the run's summary artifact or query `audit.dq_log`. Not retried automatically. |

Use **Retry** on the failed flow run in the UI, or the backfill deployment to reprocess a window. To reprocess everything, set `last_watermark` to `1900-01-01` and run the daily deployment.

## Design notes (good interview talking points)
- **Idempotent**: Bronze MERGE on `(order_id, updated_at)`, Silver MERGE keyed on `order_id` updating only when `s.updated_at >= t.updated_at`.
- **Incremental**: the watermark moves only after the Bronze write succeeds, with a 10-minute overlap. Limitations: a row that changes without bumping `updated_at` is invisible, and deletes are not captured. Real fix: CDC (Debezium).
- **SCD2** uses a dbt `timestamp` snapshot on `customers.updated_at`; `fact_orders` joins the customer version valid at purchase time.
- **Quarantine**, not silent drops: invalid rows go to `lake/silver/_quarantine/<table>` with a reason.
- **Data skew**: the pipeline itself does not hit it (joins run in Postgres via dbt on uniform data); the skew demo shows the problem and the fixes (broadcast, salting, AQE).
- **Scaling path**: Spark local -> Databricks/EMR, local folders -> S3/ADLS, Postgres -> Snowflake/Redshift/BigQuery, `serve` -> a Prefect work pool with Docker/Kubernetes workers. The flow code stays the same.
- **Simplifications vs production**: single environment; secrets in compose env vars (use a secrets manager); Prefect server on SQLite (use Postgres); runner container is root; Silver is recomputed from all of Bronze each run.

## Troubleshooting
- **Build fails downloading** Java/jars/packages: check your internet; re-run `docker compose build`.
- **Deployments missing in the UI**: `docker compose logs pipeline` (it waits for the server to be healthy, then registers them). `docker compose restart pipeline` retries.
- **Run stays "Late" or "Scheduled"**: the runner is busy with another run (`limit=1`) or the `pipeline` container is down.
- **A run fails immediately with missing CSVs**: run `ecommerce-setup/run` first.
- **Spark out-of-memory / container killed**: raise Docker memory to 8 GB, or lower `n_orders` / `rows`.
- **Port 5433/4200/4040 already in use**: change the left side of the port mapping in `docker-compose.yml`.
- **`init.sql` changes not applied**: it only runs on a fresh volume; use `docker compose down -v`.
