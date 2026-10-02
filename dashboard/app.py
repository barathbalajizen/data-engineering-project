"""Streamlit dashboard for the e-commerce pipeline.

Data source:
  - live:  Postgres warehouse (when PG_HOST is set, e.g. inside docker compose)
  - csv:   docs/sample_output/*.csv, the committed snapshot (Streamlit Community Cloud, or a fresh clone)

Run:  docker compose --profile dashboard up -d   -> http://localhost:8501
      or locally:  pip install -r dashboard/requirements.txt && streamlit run dashboard/app.py
"""
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
CSV_DIR = Path(os.getenv("SHOWCASE_DIR", ROOT / "docs" / "sample_output"))
NAMES = ["row_counts", "metadata", "agg_category_revenue", "agg_daily_sales", "agg_monthly_sales",
         "dim_customer_scd2_examples", "fact_orders_sample", "dq_log_latest_run"]

st.set_page_config(page_title="E-Commerce Pipeline Dashboard", page_icon="📊", layout="wide")


@st.cache_data(ttl=300)
def load_live() -> dict:
    sys.path.insert(0, str(ROOT / "src"))
    from common import get_engine
    from export_showcase import load_all
    with get_engine().connect() as c:
        return load_all(c)


@st.cache_data
def load_csv() -> dict:
    return {n: pd.read_csv(CSV_DIR / f"{n}.csv") for n in NAMES if (CSV_DIR / f"{n}.csv").exists()}


def load() -> tuple[dict, str]:
    if os.getenv("PG_HOST"):
        try:
            return load_live(), "Live: Postgres warehouse"
        except Exception as exc:  # warehouse down or not loaded yet -> fall back to the snapshot
            st.warning(f"Postgres not available ({type(exc).__name__}), showing the committed snapshot.")
    return load_csv(), "Snapshot: docs/sample_output"


data, source = load()
if "agg_monthly_sales" not in data:
    st.error("No data found. Run the pipeline, then the `ecommerce-export-showcase/run` deployment.")
    st.stop()

meta = data["metadata"].iloc[0] if "metadata" in data else {}
st.title("E-Commerce Analytics Pipeline")
st.caption(f"{source} · exported {meta.get('exported_at', 'n/a')} · orders watermark {meta.get('orders_watermark', 'n/a')}"
           " · Postgres → Delta Bronze/Silver → dbt Gold")

# ------------------------------------------------------------------ KPIs
monthly = data["agg_monthly_sales"]
dq = data.get("dq_log_latest_run", pd.DataFrame(columns=["status"]))
orders, revenue, late = monthly["orders"].sum(), monthly["revenue"].sum(), monthly["late_deliveries"].sum()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Orders", f"{orders:,.0f}")
k2.metric("Revenue", f"{revenue:,.0f}")
k3.metric("Late deliveries", f"{late:,.0f}")
k4.metric("Quality checks passed", f"{(dq['status'] == 'PASS').sum()} / {len(dq)}")

# ------------------------------------------------------------------ sales
st.subheader("Monthly sales")
m = monthly.set_index("month")
c1, c2 = st.columns(2)
c1.line_chart(m["revenue"], y_label="Revenue")
c2.bar_chart(m["orders"], y_label="Orders")

c1, c2 = st.columns(2)
with c1:
    st.subheader("Revenue by category")
    cat = data["agg_category_revenue"].set_index("category").sort_values("revenue", ascending=False)
    st.bar_chart(cat["revenue"].head(15), horizontal=True)
with c2:
    st.subheader("Daily sales (latest days)")
    daily = data["agg_daily_sales"].sort_values("date_day").set_index("date_day")
    st.line_chart(daily[["revenue"]])
    st.caption(f"Late deliveries in this period: {daily['late_deliveries'].sum():,.0f}")

st.dataframe(data["agg_category_revenue"], hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ pipeline
st.subheader("Data quality checks (latest run)")
st.caption("Reconciliation and quality checks written to audit.dq_log by src/checks.py.")
st.dataframe(dq.style.map(lambda v: {"PASS": "color: green", "FAIL": "color: red", "WARN": "color: orange"}
                          .get(v, ""), subset=["status"]) if len(dq) else dq,
             hide_index=True, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    st.subheader("Rows per layer")
    st.bar_chart(data["row_counts"].set_index("table")["rows"], horizontal=True)
with c2:
    st.subheader("SCD Type 2: customer history")
    st.caption("Customers who moved city: the old row is closed (valid_to), the new one is current.")
    st.dataframe(data["dim_customer_scd2_examples"], hide_index=True, use_container_width=True)

with st.expander("Sample of fact_orders (one row per order item)"):
    st.dataframe(data["fact_orders_sample"], hide_index=True, use_container_width=True)
