"""Streamlit dashboard for the e-commerce pipeline: business KPIs, pipeline operations and data quality.

Data source (same datasets either way, defined in src/export_showcase.py QUERIES):
  - live:  Postgres warehouse (when PG_HOST is set, e.g. inside docker compose)
  - csv:   docs/sample_output/*.csv, the committed snapshot (Streamlit Community Cloud, or a fresh clone)

Run:  docker compose --profile dashboard up -d   -> http://localhost:8501
      or locally:  pip install -r dashboard/requirements.txt && streamlit run dashboard/app.py
"""
import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import metrics as m  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CSV_DIR = Path(os.getenv("SHOWCASE_DIR", ROOT / "docs" / "sample_output"))
STATUS_COLORS = {"SUCCESS": "#2e7d32", "PASS": "#2e7d32", "WARN": "#ef6c00", "FAILED": "#c62828",
                 "FAIL": "#c62828", "ABANDONED": "#6d4c41", "RUNNING": "#1565c0"}

st.set_page_config(page_title="E-Commerce Pipeline Dashboard", page_icon="📊", layout="wide")


# ------------------------------------------------------------------ data loading
@st.cache_data(ttl=300)
def load_live() -> dict:
    sys.path.insert(0, str(ROOT / "src"))
    from common import get_engine
    from export_showcase import load_all
    eng = get_engine()
    try:
        return load_all(eng)
    finally:
        eng.dispose()


@st.cache_data(ttl=60)   # short TTL so a new export shows up without restarting the app
def load_csv(csv_dir: str) -> dict:
    return {p.stem: pd.read_csv(p) for p in sorted(Path(csv_dir).glob("*.csv"))}


def load() -> tuple[dict, str, str | None]:
    """Returns (data, source label, reason live data was not used)."""
    if not os.getenv("PG_HOST"):
        return load_csv(str(CSV_DIR)), "Snapshot: docs/sample_output", None
    try:
        data = load_live()
        if "kpi_monthly" in data:
            return data, "Live: Postgres warehouse", None
        reason = "the warehouse tables do not exist yet"
    except Exception as exc:  # warehouse down -> fall back to the snapshot
        reason = f"Postgres is not reachable ({type(exc).__name__})"
    return load_csv(str(CSV_DIR)), "Snapshot: docs/sample_output", reason


def dataset(data, name, what):
    """The dataset, or None after an explanation (an older snapshot may not contain it)."""
    df = data.get(name)
    if df is None:
        st.info(f"{what}: not available in this data source (`{name}`). Run the pipeline and "
                "`ecommerce-export-showcase/04-export-dashboard-snapshot` to refresh the snapshot.")
    return df


def colour_status(df, column="status"):
    return df.style.map(lambda v: f"color: {STATUS_COLORS.get(v, '')}; font-weight: 600" if v in STATUS_COLORS
                        else "", subset=[column])


def fmt_money(x):
    return f"{x:,.0f}"


# ------------------------------------------------------------------ business
def business_tab(data):
    kpi = dataset(data, "kpi_monthly", "Monthly KPIs")
    if kpi is None:
        return
    t = m.business_totals(kpi)
    c = st.columns(5)
    c[0].metric("Revenue", fmt_money(t["revenue"]))
    c[1].metric("Orders", f"{t['orders']:,}")
    c[2].metric("Average order value", f"{t['avg_order_value']:,.2f}")
    c[3].metric("Customers", f"{t['customers']:,}")
    c[4].metric("Returning customers", f"{t['returning_share_pct']:.1f}%",
                help="Share of monthly active customers who had ordered in an earlier month")
    st.caption("Revenue = item prices of non-canceled orders (freight excluded). A customer is a person "
               "(customer_unique_id). Source: dbt Gold models agg_daily_sales, agg_monthly_kpis.")

    daily = data.get("daily_sales")
    if daily is not None and len(daily):
        st.subheader("Daily revenue")
        d = m.with_rolling(daily)
        base = alt.Chart(d).encode(x=alt.X("date_day:T", title=None))
        st.altair_chart(
            base.mark_line(opacity=0.35).encode(y=alt.Y("revenue:Q", title="Revenue"),
                                                tooltip=["date_day:T", "revenue:Q", "orders:Q"])
            + base.mark_line(strokeWidth=2.5).encode(y="revenue_7d_avg:Q"),
            use_container_width=True)
        st.caption("Thin line: daily revenue. Thick line: 7-day average.")

    left, right = st.columns(2)
    with left:
        st.subheader("Average order value by month")
        st.altair_chart(alt.Chart(kpi).mark_line(point=True).encode(
            x=alt.X("month:O", title=None), y=alt.Y("avg_order_value:Q", title="AOV", scale=alt.Scale(zero=False)),
            tooltip=["month", "avg_order_value", "orders", "revenue"]), use_container_width=True)
    with right:
        st.subheader("New vs returning customers")
        cust = kpi[["month", "new_customers", "returning_customers"]].melt(
            id_vars="month", var_name="type", value_name="customers")
        st.altair_chart(alt.Chart(cust).mark_bar().encode(
            x=alt.X("month:O", title=None), y=alt.Y("customers:Q", title="Customers"),
            color=alt.Color("type:N", title=None), tooltip=["month", "type", "customers"]),
            use_container_width=True)

    ret = data.get("customer_retention")
    if ret is not None and len(ret):
        st.subheader("Customer retention by cohort")
        st.caption("Cohort = month of a customer's first order. Each cell: % of the cohort that ordered again N "
                   "months later (month 0 = 100% by definition and is left out of the colour scale).")
        heat = ret[(ret["months_since_first"] > 0) & (ret["months_since_first"] <= 12)]
        left, right = st.columns([3, 2])
        with left:
            st.altair_chart(alt.Chart(heat).mark_rect().encode(
                x=alt.X("months_since_first:O", title="Months since first order"),
                y=alt.Y("cohort_month:O", title="Cohort"),
                color=alt.Color("retention_pct:Q", title="Retention %", scale=alt.Scale(scheme="blues")),
                tooltip=["cohort_month", "months_since_first", "retention_pct", "active_customers", "cohort_size"]),
                use_container_width=True)
        with right:
            avg = m.average_retention(ret)
            st.altair_chart(alt.Chart(avg).mark_line(point=True).encode(
                x=alt.X("months_since_first:O", title="Months since first order"),
                y=alt.Y("retention_pct:Q", title="Average retention %"),
                tooltip=["months_since_first", "retention_pct"]), use_container_width=True)
            st.caption("Average over cohorts, weighted by cohort size.")

    st.subheader("Product sales")
    left, right = st.columns(2)
    products = data.get("product_sales_top")
    with left:
        if products is not None and len(products):
            top = products.head(15)
            st.altair_chart(alt.Chart(top).mark_bar().encode(
                x=alt.X("revenue:Q", title="Revenue"), y=alt.Y("product_id:N", sort="-x", title=None),
                color=alt.Color("category:N", legend=None),
                tooltip=["revenue_rank", "product_id", "category", "units_sold", "revenue", "avg_price"]),
                use_container_width=True)
            st.caption("Top 15 products by revenue (colour = category).")
    with right:
        cat = data.get("agg_category_revenue")
        if cat is not None and len(cat):
            st.altair_chart(alt.Chart(cat).mark_bar().encode(
                x=alt.X("revenue:Q", title="Revenue"), y=alt.Y("category:N", sort="-x", title=None),
                tooltip=["category", "orders", "revenue", "avg_delivery_days"]), use_container_width=True)
            st.caption("Revenue by category.")
    if products is not None and len(products):
        with st.expander("Top products table"):
            st.dataframe(products, hide_index=True, use_container_width=True)


# ------------------------------------------------------------------ operations
def operations_tab(data):
    runs = dataset(data, "flow_runs", "Pipeline run history")
    if runs is None:
        return
    s = m.run_stats(runs)
    c = st.columns(5)
    c[0].metric("Daily runs", s.get("runs", 0))
    c[1].metric("Success rate", f"{s.get('success_rate_pct', 0):.0f}%")
    c[2].metric("Failed runs", s.get("failed", 0))
    med = s.get("median_minutes")
    c[3].metric("Median duration", f"{med:.1f} min" if med is not None else "n/a")
    c[4].metric("Last run", s.get("last_status") or "n/a", help=f"started {s.get('last_start')}")

    st.subheader("Run history")
    cols = ["start_ts", "flow_name", "status", "duration_seconds", "task_attempts", "failed_attempts",
            "retried_attempts", "first_failed_task", "rows_inserted", "rows_rejected", "error"]
    st.dataframe(colour_status(runs[[c for c in cols if c in runs.columns]]), hide_index=True,
                 use_container_width=True, height=300)
    st.caption("rows_inserted counts every row written, including full-snapshot tables rewritten each run.")

    tasks = data.get("task_runs")
    if tasks is not None and len(tasks):
        st.subheader("Duration per task (last 15 daily runs)")
        dur = m.task_durations_by_run(tasks, runs)
        if len(dur):
            st.altair_chart(alt.Chart(dur).mark_bar().encode(
                x=alt.X("run_label:O", title="Run start (UTC)"), y=alt.Y("minutes:Q", title="Minutes"),
                color=alt.Color("task_name:N", title="Task"), tooltip=["run_label", "task_name", "minutes"]),
                use_container_width=True)
    stats = data.get("task_stats")
    if stats is not None and len(stats):
        st.subheader("Task statistics (all attempts)")
        st.dataframe(colour_status(stats, "last_status"), hide_index=True, use_container_width=True)
    loads = data.get("table_loads_latest")
    if loads is not None and len(loads):
        st.subheader("Rows per table in the latest daily run")
        st.dataframe(colour_status(loads), hide_index=True, use_container_width=True)


# ------------------------------------------------------------------ data quality
def quality_tab(data):
    score = dataset(data, "dq_scorecard", "Data quality scorecard")
    if score is None or score.empty:
        return
    score = score.assign(run_ts=pd.to_datetime(score["run_ts"])).sort_values("run_ts")
    last = score.iloc[-1]
    c = st.columns(4)
    c[0].metric("Pass rate (latest run)", f"{last['pass_rate']}%")
    c[1].metric("Health score", f"{last['health_score']}%", help="PASS counts 1, WARN counts 0.5")
    c[2].metric("Warnings", int(last["warned"]))
    c[3].metric("Failures", int(last["failed"]))

    left, right = st.columns(2)
    with left:
        st.subheader("Quality trend")
        trend = score[["run_ts", "pass_rate", "health_score"]].melt(
            id_vars="run_ts", var_name="score", value_name="percent")
        st.altair_chart(alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X("run_ts:T", title=None), y=alt.Y("percent:Q", title="%", scale=alt.Scale(zero=False)),
            color=alt.Color("score:N", title=None), tooltip=["run_ts:T", "score", "percent"]),
            use_container_width=True)
    with right:
        cats = data.get("dq_by_category_latest")
        if cats is not None and len(cats):
            st.subheader("Latest run by category")
            long = cats[["category", "passed", "warned", "failed"]].melt(
                id_vars="category", var_name="status", value_name="checks")
            st.altair_chart(alt.Chart(long).mark_bar().encode(
                x=alt.X("checks:Q", title="Checks"), y=alt.Y("category:N", title=None),
                color=alt.Color("status:N", scale=alt.Scale(domain=["passed", "warned", "failed"],
                                                            range=["#2e7d32", "#ef6c00", "#c62828"])),
                tooltip=["category", "status", "checks"]), use_container_width=True)

    checks = data.get("dq_checks_latest")
    if checks is not None and len(checks):
        st.subheader("Source-to-target reconciliation (latest run)")
        recon = checks[checks["category"] == "reconciliation"][["check_name", "status", "detail"]]
        st.dataframe(colour_status(recon), hide_index=True, use_container_width=True)
        issues = checks[checks["status"] != "PASS"]
        st.subheader(f"Warnings and failures (latest run): {len(issues)}")
        if len(issues):
            st.dataframe(colour_status(issues[["check_name", "check_source", "category", "status", "detail"]]),
                         hide_index=True, use_container_width=True)
        with st.expander(f"All {len(checks)} checks of the latest run"):
            st.dataframe(colour_status(checks), hide_index=True, use_container_width=True)

    st.subheader("Rejected records")
    rejected = data.get("rejected_records")
    if rejected is None or rejected.empty:
        st.success("No rows rejected by Silver validation in the recorded runs.")
    else:
        st.dataframe(m.rejected_by_rule(rejected), hide_index=True, use_container_width=True)
        st.caption("Rows quarantined by Silver validation rules (lake/silver/_quarantine), with sample keys.")
        with st.expander("Per run"):
            st.dataframe(rejected, hide_index=True, use_container_width=True)
    changes = data.get("schema_changes")
    if changes is not None and len(changes):
        with st.expander(f"Schema changes ({len(changes)} latest)"):
            st.dataframe(changes, hide_index=True, use_container_width=True)


# ------------------------------------------------------------------ page
data, source, live_problem = load()
if not data:
    st.error(f"No data to show: {live_problem or 'no snapshot in docs/sample_output'}.")
    st.markdown("1. Open Prefect at http://localhost:4200 and run **`ecommerce-setup/01-first-time-setup`** "
                "(loads data and runs the whole pipeline, about 5 minutes).\n2. Refresh this page.\n"
                "3. Optional: run **`ecommerce-export-showcase/04-export-dashboard-snapshot`** and commit "
                "`docs/sample_output/` so others see the data too.")
    st.stop()
if live_problem:
    st.warning(f"Showing the committed snapshot because {live_problem}.")

meta = data["metadata"].iloc[0] if "metadata" in data else {}
st.title("E-Commerce Analytics Pipeline")
st.caption(f"{source} · data as of {meta.get('exported_at', 'n/a')} · orders watermark "
           f"{meta.get('orders_watermark', 'n/a')} · Postgres → Delta Bronze/Silver → dbt Gold")

business, operations, quality = st.tabs(["📈 Business", "⚙️ Pipeline operations", "✅ Data quality"])
with business:
    business_tab(data)
with operations:
    operations_tab(data)
with quality:
    quality_tab(data)
