"""
Phase 4 SQL Analyst Queries
==========================

Collection of analyst-ready SQL queries for the Gold layer.
These queries power dashboards, reports, and business intelligence.
"""

# Revenue Analysis Queries

QUERY_DAILY_REVENUE = """
SELECT
    d.date,
    d.day_of_week,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value,
    SUM(f.order_quantity) as total_items
FROM fact_orders f
JOIN dim_date d ON f.dim_date_id = d.dim_date_id
GROUP BY d.date, d.day_of_week
ORDER BY d.date DESC;
"""

QUERY_REVENUE_BY_CATEGORY = """
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value,
    SUM(f.order_quantity) as items_sold,
    COUNT(CASE WHEN f.has_flagged_keywords THEN 1 END) as flagged_orders
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
"""

QUERY_REVENUE_BY_LOCATION = """
SELECT
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT f.order_id) as order_count,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value
FROM fact_orders f
JOIN dim_customer c ON f.dim_customer_id = c.dim_customer_id
GROUP BY c.customer_state, c.customer_city
ORDER BY total_revenue DESC
LIMIT 20;
"""

# Product Performance Queries

QUERY_TOP_PRODUCTS = """
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.order_quantity) as total_sold,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
GROUP BY p.product_id, p.product_name, p.category, p.price
ORDER BY total_revenue DESC
LIMIT 10;
"""

QUERY_WORST_PERFORMING_PRODUCTS = """
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT f.order_id) as order_count,
    COUNT(CASE WHEN f.has_flagged_keywords THEN 1 END) as flagged_orders,
    ROUND(
        100.0 * COUNT(CASE WHEN f.has_flagged_keywords THEN 1 END) /
        NULLIF(COUNT(DISTINCT f.order_id), 0),
        2
    ) as flagged_percentage,
    COUNT(CASE WHEN f.sentiment_label = 'negative' THEN 1 END) as negative_reviews,
    ROUND(AVG(f.sentiment_score)::numeric, 3) as avg_sentiment_score
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
WHERE f.sentiment_label IS NOT NULL
GROUP BY p.product_id, p.product_name, p.category
HAVING COUNT(DISTINCT f.order_id) >= 5
ORDER BY flagged_percentage DESC
LIMIT 20;
"""

QUERY_PRODUCT_INVENTORY = """
SELECT
    p.product_id,
    p.product_name,
    p.stock_quantity,
    COUNT(DISTINCT f.order_id) as orders_last_period,
    CASE
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 5 THEN 'Critical'
        WHEN p.stock_quantity < 20 THEN 'Low'
        ELSE 'Adequate'
    END as stock_status
FROM dim_product p
LEFT JOIN fact_orders f ON p.dim_product_id = f.dim_product_id
GROUP BY p.product_id, p.product_name, p.stock_quantity, p.price
ORDER BY stock_status, p.stock_quantity ASC;
"""

# Sentiment & Customer Satisfaction Queries

QUERY_SENTIMENT_DISTRIBUTION = """
SELECT
    f.sentiment_label,
    COUNT(DISTINCT f.order_id) as order_count,
    ROUND(
        100.0 * COUNT(DISTINCT f.order_id) /
        NULLIF((SELECT COUNT(DISTINCT order_id) FROM fact_orders WHERE sentiment_label IS NOT NULL), 0),
        2
    ) as percentage,
    ROUND(AVG(f.sentiment_score)::numeric, 3) as avg_sentiment_score
FROM fact_orders f
WHERE f.sentiment_label IS NOT NULL
GROUP BY f.sentiment_label
ORDER BY order_count DESC;
"""

QUERY_SATISFACTION_BY_CATEGORY = """
SELECT
    p.category,
    f.sentiment_label,
    COUNT(DISTINCT f.order_id) as order_count,
    ROUND(AVG(f.sentiment_score)::numeric, 3) as avg_sentiment
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
WHERE f.sentiment_label IS NOT NULL
GROUP BY p.category, f.sentiment_label
ORDER BY p.category,
    CASE
        WHEN f.sentiment_label = 'positive' THEN 1
        WHEN f.sentiment_label = 'neutral' THEN 2
        WHEN f.sentiment_label = 'negative' THEN 3
    END;
"""

# Trend Analysis Queries

QUERY_MONTHLY_TREND = """
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value,
    SUM(f.order_quantity) as total_items_sold
FROM fact_orders f
JOIN dim_date d ON f.dim_date_id = d.dim_date_id
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;
"""

QUERY_DOW_PATTERNS = """
SELECT
    CASE d.day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as day_name,
    d.day_of_week,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_order_value,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type
FROM fact_orders f
JOIN dim_date d ON f.dim_date_id = d.dim_date_id
GROUP BY d.day_of_week
ORDER BY d.day_of_week;
"""

QUERY_SENTIMENT_TREND = """
SELECT
    d.date,
    d.month,
    f.sentiment_label,
    COUNT(DISTINCT f.order_id) as order_count,
    ROUND(AVG(f.sentiment_score)::numeric, 3) as avg_sentiment_score
FROM fact_orders f
JOIN dim_date d ON f.dim_date_id = d.dim_date_id
WHERE f.sentiment_label IS NOT NULL
GROUP BY d.date, d.month, f.sentiment_label
ORDER BY d.date DESC, f.sentiment_label;
"""

# Data Quality Queries

QUERY_DATA_QUALITY = """
SELECT
    'fact_orders' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN sentiment_label IS NULL THEN 1 END) as null_sentiment_labels,
    COUNT(CASE WHEN dim_product_id IS NULL THEN 1 END) as null_product_ids,
    COUNT(CASE WHEN dim_customer_id IS NULL THEN 1 END) as null_customer_ids,
    COUNT(CASE WHEN dim_date_id IS NULL THEN 1 END) as null_date_ids
FROM fact_orders
UNION ALL
SELECT 'dim_product', COUNT(*), 0, 0, 0, 0 FROM dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*), 0, 0, 0, 0 FROM dim_customer
UNION ALL
SELECT 'dim_date', COUNT(*), 0, 0, 0, 0 FROM dim_date;
"""

QUERY_ROW_COUNT_AUDIT = """
SELECT
    'Gold: fact_orders' as layer_and_table,
    COUNT(*) as row_count
FROM fact_orders
UNION ALL
SELECT 'Gold: dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'Gold: dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'Gold: dim_date', COUNT(*) FROM dim_date;
"""

# Dictionary of all queries for easy access
QUERIES = {
    'daily_revenue': QUERY_DAILY_REVENUE,
    'revenue_by_category': QUERY_REVENUE_BY_CATEGORY,
    'revenue_by_location': QUERY_REVENUE_BY_LOCATION,
    'top_products': QUERY_TOP_PRODUCTS,
    'worst_products': QUERY_WORST_PERFORMING_PRODUCTS,
    'inventory': QUERY_PRODUCT_INVENTORY,
    'sentiment_distribution': QUERY_SENTIMENT_DISTRIBUTION,
    'satisfaction_by_category': QUERY_SATISFACTION_BY_CATEGORY,
    'monthly_trend': QUERY_MONTHLY_TREND,
    'dow_patterns': QUERY_DOW_PATTERNS,
    'sentiment_trend': QUERY_SENTIMENT_TREND,
    'data_quality': QUERY_DATA_QUALITY,
    'row_count_audit': QUERY_ROW_COUNT_AUDIT,
}

if __name__ == '__main__':
    print("Phase 4 SQL Analyst Queries")
    print("=" * 60)
    print("\nAvailable queries:")
    for name in QUERIES:
        print(f"  - {name}")
    print("\nUsage:")
    print("  from src.db.queries import QUERY_DAILY_REVENUE")
    print("  cursor.execute(QUERY_DAILY_REVENUE)")
