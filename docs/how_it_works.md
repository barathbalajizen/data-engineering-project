# How the pipeline works

A walkthrough of the logic for someone new to the project, in the order the data flows. Each part says **what** happens, **why**, and gives a small **example**. File names link to the code; every code file also starts with a comment that explains it.

**Contents**
1. [The big picture](#1-the-big-picture)
2. [One order, followed through every layer](#2-one-order-followed-through-every-layer)
3. [Words used in this project](#3-words-used-in-this-project)
4. [Copying the source into the lake (Bronze)](#4-copying-the-source-into-the-lake-bronze)
   - [4.1 What Bronze does](#41-what-bronze-does)
   - [4.2 Orders: the watermark and the overlap window](#42-orders-the-watermark-and-the-overlap-window)
   - [4.3 No duplicates: the insert-only MERGE](#43-no-duplicates-the-insert-only-merge)
   - [4.4 Metadata on every row](#44-metadata-on-every-row)
   - [4.5 Schema drift: when the source columns change](#45-schema-drift-when-the-source-columns-change)
   - [4.6 Backfill: re-reading a date range](#46-backfill-re-reading-a-date-range)
   - [4.7 Time travel and Change Data Feed](#47-time-travel-and-change-data-feed)
5. [Cleaning and validating with Spark (Silver)](#5-cleaning-and-validating-with-spark-silver)
   - [5.1 What Silver does, step by step](#51-what-silver-does-step-by-step)
   - [5.2 Validation rules and the quarantine](#52-validation-rules-and-the-quarantine)
   - [5.3 Keeping the latest valid version](#53-keeping-the-latest-valid-version)
   - [5.4 Orders: reading only what changed](#54-orders-reading-only-what-changed)
   - [5.5 MERGE: new, newer, same and late rows](#55-merge-new-newer-same-and-late-rows)
   - [5.6 The other tables: full snapshot](#56-the-other-tables-full-snapshot)
   - [5.7 Checking Silver against the source](#57-checking-silver-against-the-source)
6. [Loading into the warehouse (staging)](#6-loading-into-the-warehouse-staging)
7. [The warehouse layer with dbt (Gold)](#7-the-warehouse-layer-with-dbt-gold)
   - [7.1 Reading order](#71-reading-order)
   - [7.2 Staging models](#72-staging-models)
   - [7.3 Intermediate models](#73-intermediate-models)
   - [7.4 Customer history: snapshot and SCD Type 2](#74-customer-history-snapshot-and-scd-type-2)
   - [7.5 Dimensions](#75-dimensions)
   - [7.6 Facts and incremental loading](#76-facts-and-incremental-loading)
   - [7.7 Aggregates](#77-aggregates)
   - [7.8 dbt tests](#78-dbt-tests)

---

## 1. The big picture

| # | Step | Tool | What it does | Code |
|---|---|---|---|---|
| 1 | Source | PostgreSQL | The "shop" database: orders, customers, items, payments, products, sellers | [`load_source.py`](../src/load_source.py) |
| 2 | Bronze | PySpark + Delta Lake | Copies new and changed source rows into the lake, unchanged, plus metadata (when, which run, which batch) | [`extract_bronze.py`](../src/extract_bronze.py) |
| 3 | Silver | PySpark + Delta Lake | Cleans and validates the rows, keeps the latest valid version of each record, sets bad rows aside with the reason | [`transform_silver.py`](../src/transform_silver.py) |
| 4 | Staging | Spark JDBC | Copies Silver into the warehouse (Postgres schema `staging`) | [`load_warehouse.py`](../src/load_warehouse.py) |
| 5 | Gold | dbt (SQL) | Builds the star schema (facts and dimensions) and the business tables from staging | [`dbt_project/`](../dbt_project) |
| 6 | Checks | Python + dbt | Compares row counts and revenue across layers and records every result in a scorecard | [`checks.py`](../src/checks.py) |

Prefect runs steps 2 to 6 in this order every day ([`ecommerce_flows.py`](../flows/ecommerce_flows.py)). Each step only starts if the previous one succeeded, and every step can safely be run again.

Why two tools for transforming data? Spark does the heavy, technical work (reading the source, Delta Lake, validation, large volumes). dbt does the business modelling in SQL inside the warehouse, where it also gets tests, customer history and incremental loading built in.

## 2. One order, followed through every layer

Order **O1** is placed on 10 March 2018 with 2 products. It is `shipped` that day and `delivered` on 15 March.

| When | Layer | What happens to O1 |
|---|---|---|
| 10 March run | Bronze | One row for O1 (`status = shipped`, `updated_at = 2018-03-10 14:00`) |
| | Silver | O1 passes validation and is stored (1 row) |
| | Staging | O1 is copied to `staging.orders`, its 2 items to `staging.order_items` |
| | Gold | `fact_orders` gets **2 rows** (one per item). `agg_daily_sales` for 10 March counts 1 order |
| 15 March run | Bronze | O1 changed, so its `updated_at` is newer than the last run: a **second row** is added (`delivered`, `updated_at = 2018-03-15 09:00`). The first row stays: Bronze keeps every version |
| | Silver | The newer version replaces the old one: still 1 row for O1, now `delivered` |
| | Gold | O1 changed, so `fact_orders` deletes O1's 2 rows and inserts them again with the new status and `delivery_days = 5`. Orders that did not change are not touched |

## 3. Words used in this project

| Word | Meaning here |
|---|---|
| **Layer** | A stage of the data: Bronze (raw copy), Silver (clean), Gold (business model) |
| **Grain** | What one row of a table stands for, e.g. "one row per item sold" |
| **Fact** | A table of events with numbers to sum or average (sales, payments) |
| **Dimension** | A table that describes the who/what/when of a fact (customer, product, seller, date) |
| **Star schema** | Facts in the middle, joined to dimensions by keys |
| **Surrogate key** | An id made by the warehouse (here a hash such as `md5(product_id)`) used to join facts and dimensions |
| **SCD Type 2** | "Slowly changing dimension" that keeps history: a change adds a new row instead of overwriting |
| **Incremental** | Process only what is new or changed since the last run instead of everything |
| **Idempotent** | Running the same step twice gives the same result as running it once (no duplicates) |
| **Watermark** | The newest `updated_at` already extracted; the next extract starts from there |
| **Overlap window** | The extract starts 10 minutes before the watermark, to catch rows committed late |
| **Backfill** | Re-reading a past date range, e.g. to restore lost data |
| **Schema drift** | The source adds, removes or changes a column |
| **Late-arriving record** | A row that shows up after newer rows were already loaded (e.g. restored by a backfill) |
| **Quarantine** | Where rows that fail validation are kept, with the rules they broke |
| **Delta version** | Every write to a Delta table creates a new numbered version; old versions can still be read |
| **Checkpoint** | The last Bronze version Silver has already processed; the next run starts after it |
| **MERGE** | Insert new rows and update existing ones by key, in one statement |
| **Time travel** | Reading a Delta table as it was at an earlier version |
| **Change Data Feed** | Delta's record of which rows each version added or changed |

---

## 4. Copying the source into the lake (Bronze)

Code: [`extract_bronze.py`](../src/extract_bronze.py), [`windowing.py`](../src/windowing.py) (what to read), [`schema_drift.py`](../src/schema_drift.py) (column changes), [`checkpoints.py`](../src/checkpoints.py) (the watermark).

### 4.1 What Bronze does

Bronze is a **raw copy of the source** in Delta Lake: nothing is cleaned or removed. It keeps every version of every order, so the history is never lost and any later step can be rebuilt from it.

| Table | How it is read | Why |
|---|---|---|
| `orders` | **Incremental**: only the orders changed since the last run | Large and changes every day |
| The other 5 tables | **Full snapshot**: read in full, Bronze overwritten | Small; reading everything is simple and always correct |

### 4.2 Orders: the watermark and the overlap window

**The watermark** (`control.watermark`) is the newest `updated_at` already extracted. Each run reads the orders changed after it, then moves it forward.

**The overlap window.** Each run starts reading **10 minutes before** the watermark (`WATERMARK_LOOKBACK_MINUTES`). This is why:

| Time | What happens |
|---|---|
| 10:05 | Run 1 reads orders up to `updated_at` 10:00. The watermark becomes **10:00** |
| 10:07 | A slow transaction in the source finally commits order **O7** with `updated_at` **09:58** (the time the change was made, before 10:00) |
| 10:35 | Run 2 reads `updated_at >= 09:50` (10:00 minus 10 minutes), so **O7 is found** |

Without the overlap, run 2 would read `updated_at >= 10:00` and O7 would be **skipped forever**.

**The watermark is safe:**
- It moves only **after** the Bronze write succeeded. If the write fails, the next run reads the same rows again.
- It never moves **backwards**: the update only happens when the new value is later ([`checkpoints.py`](../src/checkpoints.py)). Every change is also recorded in `control.checkpoint_history` by a database trigger, with the run that made it.

### 4.3 No duplicates: the insert-only MERGE

The overlap means some rows are read twice. Bronze is written with a `MERGE` that **only inserts** rows whose **(order_id, updated_at)** is not in Bronze yet:

| Row read by run 2 | Already in Bronze? | Result |
|---|---|---|
| O5, updated 09:55 (read again because of the overlap) | yes | skipped |
| O7, updated 09:58 (the late commit) | no | **inserted** |
| O1, updated 10:20 (a new version of O1) | no | **inserted**, as a new row next to O1's older versions |

The same makes reruns, retries and backfills safe: data that is already there is never added twice.

### 4.4 Metadata on every row

| Column | Example | Used for |
|---|---|---|
| `ingestion_ts` | 2026-10-03 10:15:02 | When the row arrived; the data freshness check uses it |
| `batch_id` | `orders-20261003T101500-1a2b3c4d` | Which load wrote it (one id per table per attempt) |
| `source_system` | `postgres_shop` | Where it came from |
| `pipeline_run_id` | the Prefect run id | Which pipeline run loaded it; links to `audit.pipeline_run_log` |
| `source_table` | `source.orders` | Which source table |

### 4.5 Schema drift: when the source columns change

Before every write, the source columns are compared with the columns Bronze already has ([`schema_drift.py`](../src/schema_drift.py)):

| Change in the source | Example | What happens |
|---|---|---|
| New column | A `channel` column is added | **Allowed**: added to Bronze, empty for older rows. Silver and staging add it too |
| Narrower type | Source sends `int`, Bronze has `bigint` | **Allowed**: converted to `bigint`, no information lost |
| Removed column | `customer_city` disappears | **Stop** (or fill with empty values if `SCHEMA_DRIFT_ALLOW_REMOVED_COLUMNS=true`) |
| Any other type change | `price` arrives as text | **Stop** |

**Why stop?** A removed column or changed type usually means the source changed its meaning. Loading anyway would quietly fill reports with empty or wrong values. Instead, nothing is written to that table and the step fails with a message naming the change, so a person decides.

Every change, allowed or not, is recorded in `audit.schema_changes` and shown on the dashboard's Data quality tab.

### 4.6 Backfill: re-reading a date range

The deployment `ecommerce-backfill/03-backfill-date-range` re-extracts the orders whose `updated_at` is in a range, for example to restore data lost from Bronze.
- It reads exactly `[start, end)`, e.g. `2017-03-01 <= updated_at < 2017-04-01`.
- Long ranges are split into chunks (31 days by default), each its own task, so a failed chunk is retried on its own.
- It **does not move the watermark**: the daily run continues where it was.
- Rows already in Bronze are skipped (4.3), so a backfill can be run again safely.
- Afterwards, the downstream steps rebuild Silver, staging and Gold.

### 4.7 Time travel and Change Data Feed

- **Time travel:** every write creates a new table version. The version history is kept for **30 days**, and old data files for at least **7 days** (VACUUM never deletes newer ones), so you can always read a table as it was a week ago. The deployment `ecommerce-delta-inspect/ops-delta-time-travel-inspect` shows a table's history.
- **Change Data Feed:** switched on for Bronze orders. Delta also records which rows each version added, so Silver can read only those (5.4).

---

## 5. Cleaning and validating with Spark (Silver)

Code: [`transform_silver.py`](../src/transform_silver.py) (the steps and the rules of every table), [`validation.py`](../src/validation.py) (rule helpers), [`transforms.py`](../src/transforms.py) (deduplication and comparisons), [`incremental.py`](../src/incremental.py) (what to read).

Bronze is an exact copy of the source, mistakes included, with every version of every order. Silver is the clean version: **one correct row per record**, plus a quarantine of what was rejected and why.

### 5.1 What Silver does, step by step

| # | Step | Example |
|---|---|---|
| 1 | Read the Bronze rows to process | Orders: only the rows added since the last run. Other tables: everything |
| 2 | Trim spaces in every text column | `" sao paulo "` → `"sao paulo"` |
| 3 | Check every row against the table's rules | An order without a `customer_id` breaks `not_null:customer_id` |
| 4 | Put rows that break a rule in the **quarantine**, with the rules they broke | `lake/silver/_quarantine/orders` |
| 5 | Keep one row per key: the **latest valid** version | Two versions of O1 → keep the newest valid one |
| 6 | Write Silver and record the counts | Rows read, inserted, updated, rejected, in `audit.pipeline_run_log` |

### 5.2 Validation rules and the quarantine

Each table has its own list of rules in `SPECS` at the top of [`transform_silver.py`](../src/transform_silver.py). There are four kinds:

| Kind | Example | Meaning |
|---|---|---|
| `not_null` | `not_null("order_id", "customer_id")` | The column must have a value |
| `accepted` | `accepted("order_status", ORDER_STATUSES)` | The value must be in a list (`created`, `shipped`, `delivered` ...) |
| `castable` | `castable("price", "double")` | The value can be read as that type: `"12.5"` yes, `"abc"` no |
| `check` | `check("price_positive", "price > 0")` | Any other business rule |

A row that breaks rules is **not dropped**. It goes to the quarantine with **every** rule it broke:

| order_id | customer_id | order_status | failed_rules | reason |
|---|---|---|---|---|
| o9 | *(empty)* | shiped | `[not_null:customer_id, accepted:order_status]` | `not_null:customer_id, accepted:order_status` |

Where to see rejections:
- `lake/silver/_quarantine/<table>`: the rejected rows themselves
- `audit.rejected_records`: how many rows broke each rule in each run, with a few example keys
- the dashboard's **Data quality** tab

### 5.3 Keeping the latest valid version

The order of steps 3 and 5 matters: **validate first, then pick the latest version.**

Example: order O1 has two versions in Bronze.

| Version | updated_at | order_status | Valid? |
|---|---|---|---|
| 1 | 2018-03-10 | shipped | yes |
| 2 | 2018-03-15 | shiped *(typo)* | no: not an accepted status |

- **Validate first (what we do):** version 2 goes to the quarantine; Silver keeps version 1. O1 is still in the reports, with its last correct status.
- **Latest first (wrong):** version 2 is picked as "the latest", then rejected. O1 disappears from Silver completely.

The run log reports such orders as `superseded_by_invalid`, so nobody is surprised that Silver is one version behind for them.

### 5.4 Orders: reading only what changed

Bronze orders grows every day. Re-reading all of it every run would get slower and slower, so Silver reads only what is new.

**Delta versions.** Every write to a Delta table creates a new numbered version: 0, 1, 2 ... You can read the table as it was at any version (time travel). With **Change Data Feed** (switched on for Bronze orders) you can also ask for "only the rows added in versions 8 to 9".

**The checkpoint.** `control.silver_checkpoint` stores the last Bronze version already merged into Silver. Example: the checkpoint is 7 and Bronze is now at version 9, so this run reads versions 8 and 9 only.

**When it reads everything instead** ([`decide_mode`](../src/incremental.py)), checked in this order:

| Situation | What happens |
|---|---|
| Full refresh requested (`--full-refresh` / `SILVER_FULL_REFRESH=true`) | Read all of Bronze |
| Silver table does not exist yet | Read all |
| No checkpoint yet (first run) | Read all |
| Bronze version is lower than the checkpoint (table was recreated) | Read all |
| Bronze version equals the checkpoint | **Skip**: nothing new, nothing rewritten |
| Change Data Feed is off, or its data for those versions was cleaned up | Read all |
| Otherwise | **Incremental**: read versions checkpoint+1 to current |

Reading everything is always correct, only slower. The MERGE below gives the same result either way.

**Why the checkpoint is saved last.** The order is: write the quarantine → write Silver → move the checkpoint. If the run fails in the middle, the checkpoint still points to the old version, so the next run reads the same changes again. That is safe because the MERGE is idempotent.

### 5.5 MERGE: new, newer, same and late rows

Delta `MERGE` compares each incoming order with Silver by `order_id`. Example: Silver holds **O1 updated 2018-03-15**.

| Incoming row | Case | What happens |
|---|---|---|
| O2 (not in Silver) | new | Inserted |
| O1 updated 2018-03-20 | newer | Replaces the Silver row |
| O1 updated 2018-03-15 | same | Nothing: it is already there |
| O1 updated 2018-03-10 | **late** | **Ignored**: Silver already has newer data |

The "late" case is a **late-arriving record**: an old version that shows up after a newer one, e.g. because a backfill re-extracted an old date range. Without the "only if newer" condition it would overwrite fresh data with stale data. These rows are counted in the run log as `late_ignored`.

Because "same" changes nothing and "late" is ignored, running Silver twice on the same data changes nothing: it is **idempotent**. The quarantine works the same way: a rejected version is added only once (it is matched on `order_id` + `updated_at`).

### 5.6 The other tables: full snapshot

Customers, products, sellers, order items and payments are small. They are re-read in full every run, and Silver is **overwritten** with the latest valid version of each row. Their quarantine is overwritten too, so it always shows what is invalid **right now**. The orders quarantine, in contrast, is a log that keeps every rejected version.

### 5.7 Checking Silver against the source

After the run, [`checks.py`](../src/checks.py) compares every order in the source with Silver ([`reconcile_versions`](../src/transforms.py)):

| Result | Meaning | Severity |
|---|---|---|
| `missing` | In the source, but neither in Silver nor in the quarantine: data was lost | **Fail** |
| `quarantined_only` | Every version was rejected; it is in the quarantine | Reported |
| `stale_explained` | Silver has an older version because the newer one was rejected (see 5.3) | Reported |
| `stale_unexplained` | Silver is behind with no reason recorded, e.g. the order changed while the run was in progress | Warning |
| `ahead` | Silver is newer than the source; should never happen | Warning |

The results are saved in `audit.dq_log` and appear in the data quality scorecard.

---

## 6. Loading into the warehouse (staging)

Code: [`load_warehouse.py`](../src/load_warehouse.py).

Spark copies each Silver table into the Postgres schema `staging`, where dbt picks it up.

| Step | What happens | Why |
|---|---|---|
| 1 | Read Silver **at a fixed Delta version** | The load and its audit record refer to exactly the same data, even if Silver changes meanwhile |
| 2 | **Truncate** the staging table and write all rows (batched, parallel JDBC) | Simple and always correct: a rerun gives the same table. The incremental work happens in Silver and in the dbt facts |
| 3 | Count the rows in Postgres and compare with the rows written | If the numbers differ, the step fails instead of trusting the writer |
| 4 | A column that is new in Silver is first added with `ALTER TABLE ... ADD COLUMN` | Truncating keeps the old table definition, which would reject the unknown column. The change is recorded in `audit.schema_changes` |

---

## 7. The warehouse layer with dbt (Gold)

dbt turns SQL `SELECT` statements ("models") into tables and views in Postgres. You write the SELECT; dbt creates the table, works out the order to build them in (from `ref('other_model')`), and runs the tests.

### 7.1 Reading order

Read the models in the order the data flows. The same order is in [`dbt_project.yml`](../dbt_project/dbt_project.yml).

| # | Layer | Postgres schema | Built as | Files |
|---|---|---|---|---|
| 1 | Sources | `staging` | (loaded by Spark, not by dbt) | [`models/sources.yml`](../dbt_project/models/sources.yml) |
| 2 | Staging | `analytics_stg` | views | [`models/staging/`](../dbt_project/models/staging) |
| 3 | Intermediate | `analytics_int` | views | [`models/intermediate/`](../dbt_project/models/intermediate) |
| 4 | Snapshot | `snapshots` | history table | [`snapshots/customers_snapshot.sql`](../dbt_project/snapshots/customers_snapshot.sql) |
| 5 | Dimensions | `analytics` | tables | `dim_date`, `dim_customer`, `dim_product`, `dim_seller` |
| 6 | Facts | `analytics` | incremental tables | `fact_orders`, `fact_payments` |
| 7 | Aggregates | `analytics` | tables | `agg_daily_sales`, `agg_category_revenue`, `agg_monthly_kpis`, `agg_customer_retention`, `agg_product_sales` |
| 8 | Tests | | | [`models/*/schema.yml`](../dbt_project/models/marts/schema.yml), [`tests/`](../dbt_project/tests) |

```
staging.orders ──────► stg_orders ──────┐
staging.order_items ─► stg_order_items ─┴─► int_order_items_enriched ─┐
staging.customers ───► customers_snapshot ─► dim_customer ────────────┴─► fact_orders ─► agg_* tables
staging.products ────► stg_products ─► dim_product ─────────────────────────────────────► (joined by agg_*)
```
`fact_orders` does not join `dim_product` or `dim_seller`: it computes the same hash keys itself (see 7.5).

### 7.2 Staging models

**What:** one view per source table that only picks and renames columns ([`stg_orders.sql`](../dbt_project/models/staging/stg_orders.sql) and the others). One small rule lives here: a product without a category gets `'unknown'`.

**Why:** every later model reads these views, never the loaded tables. If a source column is renamed, only one file changes.

**Good to know about this dataset (Olist):** every order gets its own `customer_id`. The real person is `customer_unique_id`, so customer counts use that column.

### 7.3 Intermediate models

**What:** [`int_order_items_enriched`](../dbt_project/models/intermediate/int_order_items_enriched.sql) joins each order item to its order and adds:

| Column | Rule |
|---|---|
| `delivery_days` | delivery date − purchase date, in days (empty until delivered) |
| `is_late` | 1 if delivered after the estimated date, else 0 |
| `order_updated_at` | when the order last changed; decides what the fact rebuilds (see 7.6) |

[`int_order_payments`](../dbt_project/models/intermediate/int_order_payments.sql) does the same for payments.

**Why:** "item + its order" is needed by the fact and by the tests. Writing the join once keeps one definition of `is_late` and `delivery_days`.

### 7.4 Customer history: snapshot and SCD Type 2

**The problem:** a customer moves from São Paulo to Rio de Janeiro in June 2018. If we simply overwrite the city, every old order suddenly shows Rio, and "revenue by city in 2017" becomes wrong.

**The solution: keep every version.**
1. The **snapshot** ([`customers_snapshot.sql`](../dbt_project/snapshots/customers_snapshot.sql)) runs every pipeline run. It compares the customers with what it stored last time. A customer whose `updated_at` changed gets their stored row **closed** (an end date is set) and a **new row** added. Nothing is ever deleted.
2. [`dim_customer`](../dbt_project/models/marts/dim_customer.sql) presents those rows with clear names:

| customer_key | customer_id | customer_city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| k1 | C1 | sao paulo | 2017-01-10 | 2018-06-01 | false |
| k2 | C1 | rio de janeiro | 2018-06-01 | 9999-12-31 | true |

- `9999-12-31` means "still valid". A far-future date instead of an empty value lets every lookup use the same simple rule: `valid_from <= time < valid_to`.
- `customer_key` identifies one **version**; the facts store this key, not `customer_id`.

**How an order finds the right version** (in `fact_orders`): join on the customer id **and** the purchase time falling inside the version's validity:
```sql
left join dim_customer c
    on  c.customer_id = e.customer_id
    and e.order_purchase_timestamp >= c.valid_from
    and e.order_purchase_timestamp <  c.valid_to
```
An order from March 2017 matches k1 (São Paulo); an order from August 2018 matches k2 (Rio). This is called a **point-in-time join**.

### 7.5 Dimensions

| Model | One row per | Key | Notes |
|---|---|---|---|
| [`dim_date`](../dbt_project/models/marts/dim_date.sql) | day, 2016–2030 | `date_key` = `20170315` for 15 March 2017 | Generated, not loaded. Has year, month, weekday, weekend flag |
| [`dim_customer`](../dbt_project/models/marts/dim_customer.sql) | customer **version** | `customer_key` | See 7.4 |
| [`dim_product`](../dbt_project/models/marts/dim_product.sql) | product | `product_key` = `md5(product_id)` | |
| [`dim_seller`](../dbt_project/models/marts/dim_seller.sql) | seller | `seller_key` = `md5(seller_id)` | |

**Why hash keys:** `md5(product_id)` gives the same value every run, so the fact computes the same key itself and does not need to look it up in the dimension.

### 7.6 Facts and incremental loading

[`fact_orders`](../dbt_project/models/marts/fact_orders.sql): **one row per item sold** (an order with 3 products = 3 rows). Each row has the keys to all four dimensions and the numbers: price, freight, delivery days, late flag. [`fact_payments`](../dbt_project/models/marts/fact_payments.sql): one row per payment, built the same way.

**Why incremental:** rebuilding the whole fact every day is wasteful when only a few hundred orders changed. So:

- **First run:** the table does not exist yet, so everything is loaded.
- **Every later run:** only rows of new or changed orders are selected, by the [`incremental_filter`](../dbt_project/macros/incremental_predicate.sql) macro. A row is selected when **either**
  1. its order changed since the last run: `order_updated_at >=` the newest `order_updated_at` already in the table, **or**
  2. its key is not in the table yet.
- dbt then **deletes** the selected keys from the table and **inserts** the new rows (`delete+insert`). A changed order replaces its old rows instead of being added twice. That makes the step idempotent: run it twice, get the same table.

**Why rule 2 exists (late-arriving records), with an example:**

| | updated_at |
|---|---|
| Newest row already in `fact_orders` | 2018-09-01 |
| Order restored by a backfill | 2017-03-10 |

Rule 1 alone would skip the restored order forever, because 2017-03-10 is older than 2018-09-01. Rule 2 catches it, because its key is missing from the table. The test `assert_fact_orders_complete` proves nothing is missed (7.8).

**How rule 2 is written:** `LEFT JOIN` the existing table and keep the rows that found no match (`existing_rows.key IS NULL`). Postgres runs that as one fast join. An earlier version written with `OR NOT EXISTS` took over 17 minutes; the story is in [performance.md](performance.md).

**Safety net:** a unique index on each fact key (macro [`fact_index`](../dbt_project/macros/fact_index.sql)) makes Postgres itself reject a duplicate row.

### 7.7 Aggregates

Small tables for reports and the dashboard, rebuilt in full from the facts on every run. Shared rules: **canceled orders are excluded**, and **revenue = item prices** (shipping is reported separately as freight).

| Model | One row per | Answers |
|---|---|---|
| [`agg_daily_sales`](../dbt_project/models/marts/agg_daily_sales.sql) | day | How much did we sell each day? |
| [`agg_category_revenue`](../dbt_project/models/marts/agg_category_revenue.sql) | product category | Which categories earn most, and how fast are they delivered? |
| [`agg_monthly_kpis`](../dbt_project/models/marts/agg_monthly_kpis.sql) | month | Orders, new vs returning customers, revenue, average order value, % late |
| [`agg_customer_retention`](../dbt_project/models/marts/agg_customer_retention.sql) | cohort month × months later | Of the people who first bought in month X, how many came back? |
| [`agg_product_sales`](../dbt_project/models/marts/agg_product_sales.sql) | product | Best sellers: revenue rank and share of total revenue |

**New vs returning customer** (monthly KPIs): a customer is *new* in the month of their first order ever, and *returning* in any later month they order.

**Average order value** = revenue ÷ number of orders (not items).

**Cohort retention, step by step** (made-up numbers):
1. Find each customer's first order month: that is their **cohort**. 800 people first ordered in January 2017.
2. For every later month, count how many of those 800 ordered again.
3. Divide by the cohort size.

| cohort_month | months_since_first | cohort_size | active_customers | retention_pct |
|---|---|---|---|---|
| 2017-01 | 0 | 800 | 800 | 100.0 |
| 2017-01 | 1 | 800 | 20 | 2.5 |
| 2017-01 | 2 | 800 | 16 | 2.0 |

Month 0 is always 100%: everyone in the cohort ordered in that month by definition. A test checks this.

### 7.8 dbt tests

**How a dbt test works:** it is a query that looks for problems. It **passes when it returns no rows**; every row it returns is a problem.

| Kind | Where | Examples |
|---|---|---|
| Built-in tests | `schema.yml` next to the models | `unique`, `not_null`, `accepted_values` (status must be one of the known values), `relationships` (every `product_key` in the fact exists in `dim_product`) |
| Own reusable tests | [`tests/generic/`](../dbt_project/tests/generic) | `non_negative` (no prices below 0), `unique_combination` (a key made of two columns, e.g. order id + item number) |
| One-off checks | [`tests/`](../dbt_project/tests) | `assert_fact_orders_complete`: the incremental fact equals a full rebuild (nothing missing, nothing stale). `assert_revenue_reconciles`: daily revenue equals the order items it comes from. `assert_business_aggregates_reconcile`: day, month and product totals agree, and every cohort starts at 100% |

70 tests run on every pipeline run. Every result (pass, warn, fail) is saved in `audit.dq_log` and counted in the data quality scorecard. A few tests only **warn**. An example is "an order item whose order is missing", which happens when Silver quarantined that order: it is worth seeing, but should not stop the pipeline.
