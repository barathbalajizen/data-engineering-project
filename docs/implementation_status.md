# Implemented features and how they were verified

Status of every requirement of the project brief. "Verified" means it was run on the real stack and/or covered by automated tests (128 pytest tests, 70 dbt tests); the evidence is named. Known limitations are listed at the end.

## 1. Ingestion and Bronze

| Requirement | Implementation | Verified by |
|---|---|---|
| Watermark-based incremental extraction | `extract_bronze.py`, `windowing.py`, `control.watermark` | `test_windowing.py`; daily runs read only the overlap rows (audit log) |
| Overlap window for late / updated records | `WATERMARK_LOOKBACK_MINUTES` (10 min); insert-if-not-exists MERGE absorbs the repeats | `test_windowing.py`; reruns insert 0 rows |
| Batch ids and pipeline run ids | `audit.make_batch_id` (per table per attempt); Prefect flow run id as `pipeline_run_id` | `test_audit.py`; `audit.pipeline_run_log` |
| Ingestion timestamps and source metadata | `ingestion_ts`, `batch_id`, `source_system`, `pipeline_run_id`, `source_table` on every Bronze row | Real run: new columns evolved through Bronze, Silver and staging |
| Schema drift detection, controlled evolution | `schema_drift.py`: add = evolve, narrower type = cast, removal / other type change = reject; `audit.schema_changes` | `test_schema_drift.py` (11), `test_bronze_delta.py` (Delta end to end) |
| Delta time travel | Retention properties; `delta_tools.py as-of`; `load_warehouse` reads a pinned version | `test_bronze_delta.py`; `ecommerce-delta-inspect` on real tables |
| Change Data Feed | Enabled on `bronze/orders`; Silver reads it incrementally | `test_bronze_delta.py`, `test_silver_incremental.py` |

## 2. Silver

| Requirement | Implementation | Verified by |
|---|---|---|
| Cleansing and trimming | `transforms.clean_strings` | `test_transforms.py` |
| Duplicate handling | Latest valid version per key (`dedupe_latest`), duplicates counted in lineage | `test_transforms.py` |
| Primary key, null and data type validation | Named rules (`validation.py`): `not_null`, `accepted`, `castable` (try_cast), business `check`s | `test_validation.py` |
| Quarantine with error reasons | `lake/silver/_quarantine/<table>` with `failed_rules` and `reason`; `audit.rejected_records` per rule with sample keys | `test_silver_incremental.py`, dashboard test with rejected rows |
| Delta MERGE for inserts and updates | Strictly-newer versions update, new keys insert | `test_silver_incremental.py` |
| Late-arriving records | Older versions ignored and counted (`late_ignored`); invalid newer versions reported (`superseded_by_invalid`) | `test_silver_incremental.py` |
| Idempotent processing | Checkpoint moves after both writes; quarantine insert-if-not-exists; rerun and full refresh change nothing | `test_silver_incremental.py`; real reruns |
| Source-to-Silver reconciliation | Key- and version-level (`reconcile_versions`): missing, quarantined, stale (explained / unexplained) | `test_incremental.py`; `audit.dq_log` every run |

## 3. Staging and Gold

| Requirement | Implementation | Verified by |
|---|---|---|
| Separate staging and analytics schemas | `staging` (Spark JDBC landing), `analytics_stg`, `analytics_int`, `analytics` | Real runs |
| Load Silver through Spark JDBC | `load_warehouse.py`, batched + parallel, row count verified | Audit rows; benchmark |
| dbt staging and intermediate models | 6 `stg_*` and 2 `int_*` views | `dbt build`; Gold identical to the pre-restructure baseline |
| Star schema, facts and dimensions | `fact_orders`, `fact_payments`, `dim_customer`, `dim_product`, `dim_seller`, `dim_date` | 70 dbt tests |
| SCD2 with effective/expiry dates and current flag | dbt snapshot → `dim_customer` (`valid_from`, `valid_to`, `is_current`), point-in-time join | dbt tests; snapshot |
| Incremental dbt models | Facts with delete+insert and an anti-join for late-arriving keys | `assert_fact_orders_complete`; full-refresh copy identical; late rows restored |
| Business aggregates | `agg_daily_sales`, `agg_category_revenue`, `agg_monthly_kpis`, `agg_customer_retention`, `agg_product_sales` | `assert_business_aggregates_reconcile` |

## 4. Data quality and testing

| Requirement | Implementation | Verified by |
|---|---|---|
| not-null, unique, relationships, accepted-values tests | dbt (70 tests) + custom `non_negative`, `unique_combination` | Every daily run (`audit.dq_log`) |
| Freshness checks | dbt source freshness (data), `pipeline_previous_success_recent` (missed schedules) | Daily runs |
| Source vs target row counts | Source → Silver → staging → fact | Daily runs |
| Business metric reconciliation | Revenue, orders, payments: staging = Gold (critical), source ≈ Gold (warn) | Daily runs: equal to the cent |
| Failed checks stored | `audit.dq_log` (pipeline checks and dbt results) | `test_dbt_results.py` |
| Data quality scorecard | `audit.dq_scorecard`, `dq_scorecard_by_category`, dashboard tab | `test_dq_scorecard_pg.py` |

## 5. Orchestration and recovery

| Requirement | Implementation | Verified by |
|---|---|---|
| Scheduled and on-demand runs | Daily and weekly cron deployments; any deployment on demand | Real runs |
| Task dependencies | `pipeline_plan.STEPS`, each step after the previous one succeeded | `test_pipeline_plan.py`, `test_flow_resume.py` |
| Retries with delays | 3 retries, 1/2/4 min; dbt resumes with `dbt retry` | Prefect configuration |
| Failure handling | Failure/crash/cancellation hooks with webhook alert naming the failed step; stale runs marked ABANDONED | `test_audit_pg.py` |
| Partial reruns | `start_from` / `stop_after`, `resume_failed` from the audit log | `test_flow_resume.py` (real flow on a temporary Prefect server); real partial run |
| Safe watermarks and checkpoints | Monotonic watermark `UPDATE`; Silver checkpoint after writes; trigger-written history | `test_checkpoints_pg.py` |
| Execution status, durations, history | `audit.pipeline_run_log`, `v_flow_runs`, `v_task_stats` | `test_checkpoints_pg.py`; dashboard |

## 6. Audit and observability

| Requirement | Implementation | Verified by |
|---|---|---|
| Pipeline run log with all requested columns | `audit.pipeline_run_log` (run id, task, batch id, start/end, duration, source/inserted/updated/rejected rows, status, retry count, error) | `test_audit*.py`; every run |
| Data quality log kept separate | `audit.dq_log` | — |
| Batch-level audit and lineage | Table-level rows per batch; `audit.lineage` with Delta versions | Real runs |
| Structured logging | Run id and task in every line; `LOG_FORMAT=json` | Production config |

## 7. Performance

All measured; see [performance.md](performance.md). Highlights at 10x volume: the dbt incremental filter went from not finishing in 17 min to 3.3 s; staging writes from 10.6 s to 4.5 s; extract reads from 26.0 s to 13.2 s; reads of a compacted table from 25.7 s to 10.9 s; the weekly maintenance job from 2,718 s to 281 s.

## 8. Dashboard

Business (revenue, AOV, daily revenue, new vs returning customers, cohort retention, product sales), pipeline operations (run history, success/failure, durations per task) and data quality (score trend, reconciliation, rejected records, schema changes). The dashboard was tested headless in live and snapshot mode, and the calculations by `test_dashboard_metrics.py`.

## 9. DevOps

| Requirement | Implementation | Verified by |
|---|---|---|
| Docker Compose for all services | `docker-compose.yml` | Running stack |
| Separate environment configurations | `.env.example` / `.env.prod.example`, `docker-compose.prod.yml` | `docker compose config` (dev and prod) |
| Environment variables, no hardcoded credentials | Credentials required from env in Compose, code and the dbt profile | Missing credentials: Compose error and `ConfigError` |
| Automated tests in GitHub Actions | lint, unit tests, SQL validation, image builds, end-to-end | Jobs reproduced locally (see deployment.md); `actionlint` |
| Code and SQL validation | ruff; migrations on an empty Postgres (twice); dbt parse + compile | Reproduced locally |
| Docker images built in CI | `docker-build` job (build cache) | Images build locally |
| Setup, deployment, architecture docs | [README](../README.md), [deployment.md](deployment.md), [architecture.md](architecture.md) | — |

## Known limitations

- **Deletes are not captured.** A row deleted in the source stays in Bronze, Silver and the facts. A change without a new `updated_at` is invisible to the watermark. Both need change data capture (e.g. Debezium).
- **One machine.** Spark runs in local mode in a container, Delta on local folders, and Prefect's own state on SQLite. The scaling path is in [architecture.md](architecture.md).
- **Sample data.** The default dataset is synthetic: retention is flat (about 2.4% per month) and no rows fail validation. The real Olist dataset can be loaded instead.
- **Freshness warning.** With no new source data, the data-freshness check correctly warns after 24 hours.
- **Snapshot granularity.** SCD2 keeps one customer version per pipeline run (several changes within a day collapse into one).
- **Not yet run on GitHub.** The CI end-to-end job was not run on GitHub before this was written. Its steps were reproduced locally except the full stack, which shares the project folders with the running local stack.
- **No authentication** on the Prefect UI or the dashboard; put them behind a reverse proxy with authentication in production.
