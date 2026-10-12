# How the pipeline works

A walkthrough of the logic for someone new to the project, in the order the data flows. Each part says **what** happens, **why**, and gives a small **example**. File names link to the code; every code file also starts with a comment that explains it.

**Contents**
1. [The big picture](#1-the-big-picture)
2. [One order, followed through every layer](#2-one-order-followed-through-every-layer)
3. [Words used in this project](#3-words-used-in-this-project)
4. [The warehouse layer with dbt (Gold)](#4-the-warehouse-layer-with-dbt-gold)
   - [4.1 Reading order](#41-reading-order)
   - [4.2 Staging models](#42-staging-models)
   - [4.3 Intermediate models](#43-intermediate-models)
   - [4.4 Customer history: snapshot and SCD Type 2](#44-customer-history-snapshot-and-scd-type-2)
   - [4.5 Dimensions](#45-dimensions)
   - [4.6 Facts and incremental loading](#46-facts-and-incremental-loading)
   - [4.7 Aggregates](#47-aggregates)
   - [4.8 dbt tests](#48-dbt-tests)

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
| **Late-arriving record** | A row that shows up after newer rows were already loaded (e.g. restored by a backfill) |
| **Quarantine** | Where rows that fail validation are kept, with the rules they broke |

---

## 4. The warehouse layer with dbt (Gold)

dbt turns SQL `SELECT` statements ("models") into tables and views in Postgres. You write the SELECT; dbt creates the table, works out the order to build them in (from `ref('other_model')`), and runs the tests.

### 4.1 Reading order

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
`fact_orders` does not join `dim_product` or `dim_seller`: it computes the same hash keys itself (see 4.5).

### 4.2 Staging models

**What:** one view per source table that only picks and renames columns ([`stg_orders.sql`](../dbt_project/models/staging/stg_orders.sql) and the others). One small rule lives here: a product without a category gets `'unknown'`.

**Why:** every later model reads these views, never the loaded tables. If a source column is renamed, only one file changes.

**Good to know about this dataset (Olist):** every order gets its own `customer_id`. The real person is `customer_unique_id`, so customer counts use that column.

### 4.3 Intermediate models

**What:** [`int_order_items_enriched`](../dbt_project/models/intermediate/int_order_items_enriched.sql) joins each order item to its order and adds:

| Column | Rule |
|---|---|
| `delivery_days` | delivery date − purchase date, in days (empty until delivered) |
| `is_late` | 1 if delivered after the estimated date, else 0 |
| `order_updated_at` | when the order last changed; decides what the fact rebuilds (see 4.6) |

[`int_order_payments`](../dbt_project/models/intermediate/int_order_payments.sql) does the same for payments.

**Why:** "item + its order" is needed by the fact and by the tests. Writing the join once keeps one definition of `is_late` and `delivery_days`.

### 4.4 Customer history: snapshot and SCD Type 2

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

### 4.5 Dimensions

| Model | One row per | Key | Notes |
|---|---|---|---|
| [`dim_date`](../dbt_project/models/marts/dim_date.sql) | day, 2016–2030 | `date_key` = `20170315` for 15 March 2017 | Generated, not loaded. Has year, month, weekday, weekend flag |
| [`dim_customer`](../dbt_project/models/marts/dim_customer.sql) | customer **version** | `customer_key` | See 4.4 |
| [`dim_product`](../dbt_project/models/marts/dim_product.sql) | product | `product_key` = `md5(product_id)` | |
| [`dim_seller`](../dbt_project/models/marts/dim_seller.sql) | seller | `seller_key` = `md5(seller_id)` | |

**Why hash keys:** `md5(product_id)` gives the same value every run, so the fact computes the same key itself and does not need to look it up in the dimension.

### 4.6 Facts and incremental loading

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

Rule 1 alone would skip the restored order forever, because 2017-03-10 is older than 2018-09-01. Rule 2 catches it, because its key is missing from the table. The test `assert_fact_orders_complete` proves nothing is missed (4.8).

**How rule 2 is written:** `LEFT JOIN` the existing table and keep the rows that found no match (`existing_rows.key IS NULL`). Postgres runs that as one fast join. An earlier version written with `OR NOT EXISTS` took over 17 minutes; the story is in [performance.md](performance.md).

**Safety net:** a unique index on each fact key (macro [`fact_index`](../dbt_project/macros/fact_index.sql)) makes Postgres itself reject a duplicate row.

### 4.7 Aggregates

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

### 4.8 dbt tests

**How a dbt test works:** it is a query that looks for problems. It **passes when it returns no rows**; every row it returns is a problem.

| Kind | Where | Examples |
|---|---|---|
| Built-in tests | `schema.yml` next to the models | `unique`, `not_null`, `accepted_values` (status must be one of the known values), `relationships` (every `product_key` in the fact exists in `dim_product`) |
| Own reusable tests | [`tests/generic/`](../dbt_project/tests/generic) | `non_negative` (no prices below 0), `unique_combination` (a key made of two columns, e.g. order id + item number) |
| One-off checks | [`tests/`](../dbt_project/tests) | `assert_fact_orders_complete`: the incremental fact equals a full rebuild (nothing missing, nothing stale). `assert_revenue_reconciles`: daily revenue equals the order items it comes from. `assert_business_aggregates_reconcile`: day, month and product totals agree, and every cohort starts at 100% |

70 tests run on every pipeline run. Every result (pass, warn, fail) is saved in `audit.dq_log` and counted in the data quality scorecard. A few tests only **warn**. An example is "an order item whose order is missing", which happens when Silver quarantined that order: it is worth seeing, but should not stop the pipeline.
