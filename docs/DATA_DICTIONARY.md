# E-Commerce Sales & Customer Feedback Pipeline - Data Dictionary

## Overview
This document describes the data structures, schemas, and transformations throughout the pipeline (Bronze → Silver → Gold).

---

## Bronze Layer (Raw Data)

Raw data landing zone - **as-is, no transformations**.

### `bronze/orders/`
**Source**: CSV batch export  
**Partitioning**: `source=orders/date=YYYY-MM-DD`

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier |
| product_id | string | Product reference (foreign key) |
| customer_id | string | Customer identifier |
| customer_state | string | State/region code (e.g., "SP", "RJ") |
| customer_city | string | City name |
| customer_zip | string | ZIP/postal code |
| order_date | date | Order date (ISO 8601) |
| order_quantity | int | Number of items ordered |
| order_value | decimal | Price per unit (USD) |
| shipping_cost | decimal | Shipping charge (USD) |
| total_amount | decimal | order_value * order_quantity + shipping_cost |
| _ingestion_timestamp | timestamp | When record was ingested (UTC) |

---

### `bronze/reviews/`
**Source**: CSV with customer feedback  
**Partitioning**: `source=reviews/date=YYYY-MM-DD`

| Column | Type | Description |
|--------|------|-------------|
| review_id | string | Unique review identifier |
| review_comment | string | Unstructured text feedback |
| order_id | string | Order this review refers to |
| product_id | string | Product being reviewed |
| review_creation_date | date | When review was posted |
| _ingestion_timestamp | timestamp | When record was ingested (UTC) |

---

### `bronze/products/`
**Source**: REST API (DummyJSON)  
**Partitioning**: `source=products/date=YYYY-MM-DD`

| Column | Type | Description |
|--------|------|-------------|
| product_id | string | Unique product ID |
| product_name | string | Display name |
| category | string | Product category (e.g., "Electronics") |
| price | decimal | List price (USD) |
| stock_quantity | int | Current inventory |
| _ingestion_timestamp | timestamp | When record was ingested (UTC) |

---

## Silver Layer (Cleaned & Typed)

Standardized, deduplicated data with business rules applied.

### `silver/orders/`
**Transformations applied**:
- Type casting (date strings → dates)
- State codes uppercase
- Deduplication (keep most recent by timestamp)
- Null/negative price validation

| Column | Type | Changes from Bronze |
|--------|------|---------------------|
| order_id | string | ✓ Deduplicated |
| product_id | string | ✓ Null handling |
| customer_id | string | ✓ Null handling |
| customer_state | string | ✓ Uppercase |
| customer_city | string | ✓ Null → "Unknown" |
| customer_zip | string | ✓ Null → "Unknown" |
| order_date | date | ✓ Parsed to date |
| order_quantity | int | ✓ Type enforced |
| order_value | decimal | ✓ Negative validation |
| shipping_cost | decimal | ✓ Negative validation |
| total_amount | decimal | ✓ Calculated/validated |

---

### `silver/reviews_enriched/`
**Transformations applied**:
- Type casting
- Sentiment analysis (VADER)
- Keyword flagging (regex patterns)
- Null handling

| Column | Type | Changes from Bronze |
|--------|------|---------------------|
| review_id | string | ✓ Preserved |
| review_comment | string | ✓ Trimmed |
| order_id | string | ✓ Preserved |
| product_id | string | ✓ Preserved |
| review_creation_date | date | ✓ Parsed to date |
| **sentiment_score** | decimal | **NEW**: [-1, 1] VADER score |
| **sentiment_label** | string | **NEW**: "positive" \| "neutral" \| "negative" |
| **flagged_keywords** | array | **NEW**: ["broken", "late", "refund"] if present |

**Sentiment Rules**:
- `sentiment_score >= 0.05` → "positive"
- `-0.05 <= sentiment_score < 0.05` → "neutral"
- `sentiment_score < -0.05` → "negative"

**Flagged Keywords**: broken, defective, late, delay, refund, damaged, wrong, issue

---

### `silver/products/`
**Transformations applied**:
- Type casting
- Category normalization
- Stock handling

| Column | Type | Changes from Bronze |
|--------|------|---------------------|
| product_id | string | ✓ Preserved |
| product_name | string | ✓ Trimmed |
| category | string | ✓ Standardized |
| price | decimal | ✓ Validated non-negative |
| stock_quantity | int | ✓ Null → 0 |

---

## Gold Layer (Star Schema)

Dimensional model optimized for analytics and BI tools.

### Fact Table: `fact_orders`

**Grain**: One row per order  
**Partitioning**: `date=YYYY-MM-DD`

| Column | Type | Role | Description |
|--------|------|------|-------------|
| order_id | string | PK | Unique order ID |
| dim_customer_id | int | FK | Foreign key to dim_customer |
| dim_product_id | int | FK | Foreign key to dim_product |
| dim_date_id | int | FK | Foreign key to dim_date |
| order_quantity | int | Measure | Units sold |
| order_value | decimal | Measure | Price per unit |
| shipping_cost | decimal | Measure | Shipping charge |
| total_amount | decimal | Measure | Total revenue |
| sentiment_score | decimal | Dimension | Review sentiment [-1, 1] |
| sentiment_label | string | Dimension | "positive" \| "neutral" \| "negative" |
| flagged_keywords | array | Dimension | Alert flags from reviews |

---

### Dimension: `dim_product`

**Grain**: One row per unique product  
**Type**: Slowly Changing Dimension (Type 1)

| Column | Type | Description |
|--------|------|-------------|
| dim_product_id | int | Surrogate key (auto-increment) |
| product_id | string | Natural key from source |
| product_name | string | Product display name |
| category | string | Category classification |
| price | decimal | Current unit price (USD) |
| stock_quantity | int | Current inventory level |
| is_active | boolean | Is product currently sold? |
| created_at | timestamp | When record was created |
| updated_at | timestamp | Last update timestamp |
| _source | string | Source system identifier |

---

### Dimension: `dim_customer`

**Grain**: One row per unique customer  
**Type**: Slowly Changing Dimension (Type 1)

| Column | Type | Description |
|--------|------|-------------|
| dim_customer_id | int | Surrogate key (auto-increment) |
| customer_id | string | Natural key from source |
| customer_state | string | State/region (normalized) |
| customer_city | string | City name |
| customer_zip | string | ZIP code |
| is_active | boolean | Is customer currently active? |
| created_at | timestamp | When record was created |
| updated_at | timestamp | Last update timestamp |
| _source | string | Source system identifier |

---

### Dimension: `dim_date`

**Grain**: One row per calendar day  
**Type**: Static dimension (does not change after creation)

| Column | Type | Description |
|--------|------|-------------|
| dim_date_id | int | Natural key (YYYYMMDD format) |
| date | date | Calendar date |
| year | int | Year (2024, 2025, etc.) |
| quarter | int | Quarter (1-4) |
| month | int | Month (1-12) |
| day | int | Day of month (1-31) |
| day_of_week | int | Day of week (0=Monday, 6=Sunday) |
| is_weekend | boolean | Saturday or Sunday? |
| is_holiday | boolean | Is public holiday? (currently false) |
| created_at | timestamp | When record was created |

---

## Example Query: Revenue by Category

```sql
SELECT 
    p.category,
    d.year,
    d.month,
    SUM(f.total_amount) AS revenue,
    COUNT(DISTINCT f.order_id) AS order_count,
    AVG(f.sentiment_score) AS avg_sentiment
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
JOIN dim_date d ON f.dim_date_id = d.dim_date_id
WHERE d.year = 2024
GROUP BY p.category, d.year, d.month
ORDER BY d.month, revenue DESC;
```

---

## Data Quality Rules

| Layer | Rule | Action |
|-------|------|--------|
| **Bronze** | None (raw landing) | Store as-is |
| **Silver** | No negative prices | Reject order |
| **Silver** | No null order IDs | Reject order |
| **Silver** | Valid date ranges | Reject invalid dates |
| **Silver** | Sentiment score ∈ [-1, 1] | Reject invalid scores |
| **Gold** | FK referential integrity | Validate before insert |
| **Gold** | No duplicate order IDs | Enforce primary key |

---

## Refresh Cadence

| Layer | Frequency | Mode |
|-------|-----------|------|
| Bronze | Daily (morning) | Batch ingestion |
| Silver | Daily (morning) | Transformation |
| Gold | Daily (morning) | Dimensional load |
| BI Layer | 1-hour intervals | Incremental sync |

---

## Data Lineage

```
CSV Orders (Bronze)      DummyJSON API (Bronze)      CSV Reviews (Bronze)
    ↓                            ↓                             ↓
    └─→ silver/orders ←─────────┘                 ─→ silver/reviews_enriched
         (typed, deduped)        (typed, enriched with sentiment)
              ↓                                            ↓
              └────────────────────────┬─────────────────┘
                                       ↓
                          dim_customer + dim_date
                                       ↓
                                 fact_orders
                                (Gold layer)
                                       ↓
                            BI Tools / Dashboard
```

---

## Glossary

| Term | Definition |
|------|-----------|
| **Bronze** | Raw data landing zone (as-is from sources) |
| **Silver** | Cleaned, typed, deduplicated data |
| **Gold** | Business-ready star schema for BI |
| **Grain** | Level of detail (one row per order, per product, etc.) |
| **SCD** | Slowly Changing Dimension (how dimension updates are handled) |
| **Surrogate Key** | Auto-generated integer primary key (not from source) |
| **Natural Key** | Business identifier from source system |
| **Fact** | Event/transaction table (many rows, metrics) |
| **Dimension** | Reference/lookup table (few rows, descriptions) |

---

## Contact & Updates

For questions about this schema:
- Data Engineer: [your-name]
- Last Updated: 2024-01-XX
- Version: 1.0
