-- E-Commerce Star Schema for Gold Layer
-- fact_orders table + dimension tables (dim_product, dim_customer, dim_date)

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_product (
    dim_product_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock_quantity INT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR(50)
);

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customer (
    dim_customer_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_state VARCHAR(10),
    customer_city VARCHAR(100),
    customer_zip VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR(50)
);

-- Dimension: Date (for time-series analysis)
CREATE TABLE IF NOT EXISTS dim_date (
    dim_date_id INT PRIMARY KEY,  -- YYYYMMDD format
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table: daily orders with sentiment
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_order_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    dim_product_id INT NOT NULL REFERENCES dim_product(dim_product_id),
    dim_customer_id INT NOT NULL REFERENCES dim_customer(dim_customer_id),
    dim_date_id INT NOT NULL REFERENCES dim_date(dim_date_id),

    -- Measures
    order_quantity INT NOT NULL DEFAULT 1,
    order_value DECIMAL(10, 2) NOT NULL,
    shipping_cost DECIMAL(10, 2),
    total_amount DECIMAL(10, 2) NOT NULL,

    -- Quality measures
    review_sentiment VARCHAR(20),  -- positive, neutral, negative
    sentiment_score FLOAT,  -- -1 to 1
    has_flagged_keywords BOOLEAN DEFAULT FALSE,
    flagged_keywords VARCHAR(500),

    -- Metadata
    order_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _ingestion_timestamp TIMESTAMP,
    _source VARCHAR(50)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date ON fact_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_dim_product_id ON fact_orders(dim_product_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_dim_customer_id ON fact_orders(dim_customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_sentiment ON fact_orders(review_sentiment);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_state ON dim_customer(customer_state);

-- Sample analytical queries (stored as views)
CREATE OR REPLACE VIEW vw_daily_revenue AS
SELECT
    f.order_date,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(DISTINCT f.dim_customer_id) as unique_customers,
    SUM(f.total_amount) as daily_revenue,
    AVG(f.total_amount) as avg_order_value
FROM fact_orders f
GROUP BY f.order_date
ORDER BY f.order_date DESC;

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    SUM(CASE WHEN f.review_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
    ROUND(
        100.0 * SUM(CASE WHEN f.review_sentiment = 'negative' THEN 1 ELSE 0 END) /
        NULLIF(COUNT(DISTINCT f.order_id), 0),
        2
    ) as negative_review_pct
FROM fact_orders f
JOIN dim_product p ON f.dim_product_id = p.dim_product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_sentiment_summary AS
SELECT
    f.order_date,
    f.review_sentiment,
    COUNT(*) as review_count,
    ROUND(AVG(f.sentiment_score)::numeric, 2) as avg_sentiment_score
FROM fact_orders f
WHERE f.review_sentiment IS NOT NULL
GROUP BY f.order_date, f.review_sentiment
ORDER BY f.order_date DESC, f.review_sentiment;
