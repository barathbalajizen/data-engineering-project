"""Datasets for the Streamlit dashboard, and an exported snapshot of them for GitHub / Streamlit Cloud.

QUERIES is the single list of what the dashboard shows. In live mode the dashboard runs these queries
against Postgres (load_all); `python export_showcase.py` writes the same results to docs/sample_output/
(one CSV per dataset, rendered as tables on GitHub, plus a README.md), which the dashboard reads when no
database is available. Business metrics come from dbt Gold models; operations and data quality from the
audit schema.

Usage: python export_showcase.py   (Prefect deployment ecommerce-export-showcase/run)
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
    ("analytics", "agg_monthly_kpis"), ("analytics", "agg_customer_retention"), ("analytics", "agg_product_sales"),
]

LATEST_DQ_RUN = "(SELECT run_id FROM audit.dq_log ORDER BY id DESC LIMIT 1)"
LATEST_DAILY_RUN = ("(SELECT pipeline_run_id FROM audit.pipeline_run_log WHERE level = 'flow' "
                    "AND task_name = 'ecommerce-daily' ORDER BY id DESC LIMIT 1)")

# name -> (section, description, SQL)
QUERIES = {
    # ------------------------------------------------------------------ business (dbt Gold)
    "kpi_monthly": ("Business", "Monthly KPIs: orders, new vs returning customers, revenue, average order value",
                    "SELECT to_char(month, 'YYYY-MM') AS month, orders, customers, new_customers, "
                    "returning_customers, items_sold, revenue, freight, avg_order_value, late_delivery_pct "
                    "FROM analytics.agg_monthly_kpis ORDER BY month"),
    "daily_sales": ("Business", "Daily revenue, orders and average order value",
                    "SELECT date_day, orders, items_sold, round(revenue::numeric, 2) AS revenue, "
                    "round(freight::numeric, 2) AS freight, late_deliveries, "
                    "round((revenue / nullif(orders, 0))::numeric, 2) AS avg_order_value "
                    "FROM analytics.agg_daily_sales ORDER BY date_day"),
    "customer_retention": ("Business", "Monthly cohort retention (% of a cohort ordering again N months later)",
                           "SELECT to_char(cohort_month, 'YYYY-MM') AS cohort_month, months_since_first, "
                           "cohort_size, active_customers, retention_pct "
                           "FROM analytics.agg_customer_retention ORDER BY cohort_month, months_since_first"),
    "product_sales_top": ("Business", f"Top {SAMPLE_ROWS} products by revenue",
                          "SELECT revenue_rank, product_id, category, orders, units_sold, revenue, avg_price, "
                          "revenue_share_pct, first_sale, last_sale FROM analytics.agg_product_sales "
                          f"ORDER BY revenue_rank LIMIT {SAMPLE_ROWS}"),
    "agg_category_revenue": ("Business", "Revenue, orders and average delivery days per product category",
                             "SELECT category, orders, round(revenue::numeric, 2) AS revenue, "
                             "round(avg_delivery_days::numeric, 1) AS avg_delivery_days "
                             "FROM analytics.agg_category_revenue ORDER BY revenue DESC"),
    # ------------------------------------------------------------------ pipeline operations (audit)
    "flow_runs": ("Operations", "Pipeline run history (latest 200 flow runs)",
                  "SELECT pipeline_run_id, flow_name, status, start_ts, end_ts, duration_seconds, task_attempts, "
                  "failed_attempts, retried_attempts, first_failed_task, rows_inserted, rows_updated, "
                  "rows_rejected, left(error_details, 300) AS error FROM audit.v_flow_runs "
                  "ORDER BY start_ts DESC LIMIT 200"),
    "task_runs": ("Operations", "Task attempts with durations (latest 1000)",
                  "SELECT pipeline_run_id, task_name, status, start_ts, duration_seconds, retry_count "
                  "FROM audit.pipeline_run_log WHERE level = 'task' ORDER BY start_ts DESC LIMIT 1000"),
    "task_stats": ("Operations", "Per task: success rate and duration statistics",
                   "SELECT task_name, attempts, successes, failures, success_rate, avg_seconds, median_seconds, "
                   "p95_seconds, max_seconds, last_start, last_status FROM audit.v_task_stats ORDER BY task_name"),
    "table_loads_latest": ("Operations", "Rows per table in the latest daily run",
                           "SELECT task_name, table_name, source_row_count AS rows_read, inserted_count AS inserted, "
                           "updated_count AS updated, rejected_count AS rejected, duration_seconds, status "
                           "FROM audit.pipeline_run_log WHERE level = 'table' "
                           f"AND pipeline_run_id = {LATEST_DAILY_RUN} ORDER BY id"),
    # ------------------------------------------------------------------ data quality (audit)
    "dq_scorecard": ("Data quality", "Data quality score per run (latest 100 runs)",
                     "SELECT run_id, run_ts, checks, passed, warned, failed, pass_rate, health_score "
                     "FROM audit.dq_scorecard ORDER BY run_ts DESC LIMIT 100"),
    "dq_by_category_latest": ("Data quality", "Latest run: checks per category",
                              "SELECT category, checks, passed, warned, failed, pass_rate "
                              "FROM audit.dq_scorecard_by_category "
                              f"WHERE run_id = {LATEST_DQ_RUN} ORDER BY category"),
    "dq_checks_latest": ("Data quality", "Latest run: every check (pipeline checks and dbt tests)",
                         "SELECT check_name, check_source, category, status, rows_failed, detail, run_ts "
                         f"FROM audit.dq_latest WHERE run_id = {LATEST_DQ_RUN} "
                         "ORDER BY status <> 'FAIL', status <> 'WARN', category, check_name"),
    "rejected_records": ("Data quality", "Rows rejected by Silver validation, per rule (latest 200)",
                         "SELECT recorded_at, pipeline_run_id, table_name, rule, rows_rejected, sample_keys "
                         "FROM audit.rejected_records ORDER BY id DESC LIMIT 200"),
    "schema_changes": ("Data quality", "Detected schema changes and what the pipeline did (latest 50)",
                       "SELECT detected_at, table_name, change_type, column_name, old_type, new_type, action "
                       "FROM audit.schema_changes ORDER BY id DESC LIMIT 50"),
    # ------------------------------------------------------------------ model examples
    "dim_customer_scd2_examples": ("Model", "SCD Type 2: customers with more than one version",
                                   "SELECT customer_id, customer_city, customer_state, valid_from, valid_to, "
                                   "is_current FROM analytics.dim_customer WHERE customer_id IN ("
                                   "SELECT customer_id FROM analytics.dim_customer GROUP BY 1 HAVING count(*) > 1 "
                                   f"ORDER BY 1 LIMIT {SAMPLE_ROWS // 2}) ORDER BY customer_id, valid_from"),
    "fact_orders_sample": ("Model", f"Sample of {SAMPLE_ROWS} fact rows (grain: one row per order item)",
                           "SELECT order_id, order_item_id, date_key, order_status, order_purchase_timestamp, "
                           "price, freight_value, delivery_days, is_late FROM analytics.fact_orders "
                           f"ORDER BY order_purchase_timestamp DESC LIMIT {SAMPLE_ROWS}"),
}


def md_table(df: pd.DataFrame, max_rows: int = 15) -> str:
    df = df.head(max_rows).astype(str)
    lines = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    lines += ["| " + " | ".join(row) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def _read(eng, sql):
    with eng.connect() as c:   # own connection per query: one failing query cannot abort the others
        return pd.read_sql(sql, c)


def load_all(eng) -> dict:
    """Run every dataset query. Returns {name: DataFrame}. A dataset whose tables do not exist yet (e.g. a
    database older than a migration) is left out with a warning instead of failing everything."""
    data = {}
    counts = []
    for schema, table in COUNTS:
        try:
            counts.append((f"{schema}.{table}", int(_read(eng, f"SELECT count(*) AS n FROM {schema}.{table}")["n"][0])))
        except Exception as exc:
            log.warning("row count of %s.%s skipped: %s", schema, table, str(exc).splitlines()[0][:150])
    data["row_counts"] = pd.DataFrame(counts, columns=["table", "rows"])
    for name, (_, _, sql) in QUERIES.items():
        try:
            data[name] = _read(eng, sql)
        except Exception as exc:
            log.warning("dataset %s skipped: %s", name, str(exc).splitlines()[0][:150])
    try:
        wm = _read(eng, "SELECT last_watermark FROM control.watermark WHERE table_name = 'orders'")
        watermark = str(wm["last_watermark"][0]) if len(wm) else "n/a"
    except Exception:
        watermark = "n/a"
    data["metadata"] = pd.DataFrame([{"exported_at": f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
                                      "orders_watermark": watermark}])
    return data


def main():
    os.makedirs(OUT, exist_ok=True)
    eng = get_engine()
    try:
        data = load_all(eng)
    finally:
        eng.dispose()
    for name, df in data.items():
        df.to_csv(f"{OUT}/{name}.csv", index=False)
        log.info("%s.csv: %d rows", name, len(df))
    meta = data["metadata"].iloc[0]
    md = ["# Sample output", "",
          "A snapshot of what the pipeline produces, exported from the Postgres warehouse by "
          "`src/export_showcase.py` (Prefect deployment `ecommerce-export-showcase/run`). The Streamlit dashboard "
          "shows the same datasets. Regenerate it after a run and commit it to refresh this page.", "",
          f"- Exported: {meta['exported_at']}", f"- Orders watermark: {meta['orders_watermark']}", "",
          "## Row counts per layer", "", md_table(data["row_counts"], max_rows=len(data["row_counts"]))]
    section = None
    for name, (sec, desc, _) in QUERIES.items():
        if name not in data:
            continue
        if sec != section:
            md += ["", f"## {sec}"]
            section = sec
        df = data[name]
        md += ["", f"### {desc}", "", f"Full file: [{name}.csv]({name}.csv) ({len(df)} rows)", ""]
        md.append(md_table(df) if len(df) else "_No rows._")
    with open(f"{OUT}/README.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    log.info("Showcase written to %s", OUT)


if __name__ == "__main__":
    main()
