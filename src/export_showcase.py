"""Export a small snapshot of the pipeline results to docs/sample_output/ (committed to git), so anyone
browsing the repo on GitHub can see real output without running the stack.

Writes one CSV per query (GitHub renders CSVs as tables) and a README.md summary. The Streamlit dashboard
(dashboard/app.py) reads these CSVs when Postgres is not available, e.g. on Streamlit Community Cloud.
Usage: python export_showcase.py   (run after the pipeline has loaded data)
"""
import os
from datetime import datetime, timezone

import pandas as pd

from common import get_engine, get_logger

log = get_logger("showcase")
OUT = os.getenv("SHOWCASE_DIR", "/app/docs/sample_output")
SAMPLE_ROWS = int(os.getenv("SHOWCASE_ROWS", "50"))

COUNTS = [
    ("source", "orders"), ("source", "customers"), ("staging", "orders"), ("staging", "order_items"),
    ("analytics", "fact_orders"), ("analytics", "fact_payments"), ("analytics", "dim_customer"),
    ("analytics", "dim_product"), ("analytics", "dim_seller"), ("analytics", "agg_daily_sales"),
    ("analytics", "agg_category_revenue"),
]

# file name -> (description, SQL)
QUERIES = {
    "agg_category_revenue": (
        "Revenue, orders and average delivery days per product category (all categories)",
        "SELECT category, orders, round(revenue::numeric, 2) AS revenue, "
        "round(avg_delivery_days::numeric, 1) AS avg_delivery_days "
        "FROM analytics.agg_category_revenue ORDER BY revenue DESC"),
    "agg_daily_sales": (
        f"Daily sales, latest {SAMPLE_ROWS} days",
        "SELECT date_day, orders, items_sold, round(revenue::numeric, 2) AS revenue, "
        "round(freight::numeric, 2) AS freight, late_deliveries "
        f"FROM analytics.agg_daily_sales ORDER BY date_day DESC LIMIT {SAMPLE_ROWS}"),
    "agg_monthly_sales": (
        "Monthly sales (rolled up from agg_daily_sales)",
        "SELECT to_char(date_trunc('month', date_day), 'YYYY-MM') AS month, sum(orders) AS orders, "
        "round(sum(revenue)::numeric, 2) AS revenue, sum(late_deliveries) AS late_deliveries "
        "FROM analytics.agg_daily_sales GROUP BY 1 ORDER BY 1"),
    "dim_customer_scd2_examples": (
        "SCD Type 2: customers with more than one version (old city closed, new city current)",
        "SELECT customer_id, customer_city, customer_state, valid_from, valid_to, is_current "
        "FROM analytics.dim_customer WHERE customer_id IN ("
        "  SELECT customer_id FROM analytics.dim_customer GROUP BY 1 HAVING count(*) > 1 "
        f"  ORDER BY 1 LIMIT {SAMPLE_ROWS // 2}) "
        "ORDER BY customer_id, valid_from"),
    "fact_orders_sample": (
        f"Sample of {SAMPLE_ROWS} rows from the fact table (grain: one row per order item)",
        "SELECT order_id, order_item_id, date_key, order_status, order_purchase_timestamp, "
        "price, freight_value, delivery_days, is_late "
        f"FROM analytics.fact_orders ORDER BY order_purchase_timestamp DESC LIMIT {SAMPLE_ROWS}"),
    "dq_log_latest_run": (
        "Data quality and reconciliation checks of the latest run (audit.dq_log)",
        "SELECT run_id, check_name, status, rows_failed, detail, run_ts FROM audit.dq_log "
        "WHERE run_id = (SELECT run_id FROM audit.dq_log ORDER BY id DESC LIMIT 1) ORDER BY id"),
}


def md_table(df: pd.DataFrame, max_rows: int = 15) -> str:
    df = df.head(max_rows).astype(str)
    lines = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    lines += ["| " + " | ".join(row) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def load_all(conn) -> dict:
    """Run every showcase query. Returns {name: DataFrame}; also used by the Streamlit dashboard (live mode)."""
    data = {"row_counts": pd.DataFrame(
        [(f"{s}.{t}", int(pd.read_sql(f"SELECT count(*) AS n FROM {s}.{t}", conn)["n"][0])) for s, t in COUNTS],
        columns=["table", "rows"])}
    data.update({name: pd.read_sql(sql, conn) for name, (_, sql) in QUERIES.items()})
    wm = pd.read_sql("SELECT last_watermark FROM control.watermark WHERE table_name='orders'", conn)
    data["metadata"] = pd.DataFrame([{
        "exported_at": f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
        "orders_watermark": str(wm["last_watermark"][0]) if len(wm) else "n/a"}])
    return data


def main():
    os.makedirs(OUT, exist_ok=True)
    with get_engine().connect() as c:
        data = load_all(c)
    for name, df in data.items():
        df.to_csv(f"{OUT}/{name}.csv", index=False)
        log.info("%s.csv: %d rows", name, len(df))
    counts, meta = data["row_counts"], data["metadata"].iloc[0]
    results = {name: (desc, data[name]) for name, (desc, _) in QUERIES.items()}

    md = [
        "# Sample output",
        "",
        "A snapshot of what the pipeline produces, exported from the Postgres warehouse by "
        "`src/export_showcase.py` (Prefect deployment `ecommerce-export-showcase/run`). "
        "Regenerate it after a run and commit it to refresh this page.",
        "",
        f"- Exported: {meta['exported_at']}",
        f"- Orders watermark: {meta['orders_watermark']}",
        "",
        "## Row counts per layer",
        "",
        md_table(counts, max_rows=len(counts)),
    ]
    for name, (desc, df) in results.items():
        md += ["", f"## {desc}", "", f"Full file: [{name}.csv]({name}.csv) ({len(df)} rows)", ""]
        md.append(md_table(df) if len(df) else "_No rows._")
    with open(f"{OUT}/README.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    log.info("Showcase written to %s", OUT)


if __name__ == "__main__":
    main()
