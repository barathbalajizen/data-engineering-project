"""Pure pandas helpers for the dashboard (no Streamlit, unit-tested in tests/test_dashboard_metrics.py)."""
import pandas as pd

FAILED_STATES = ("FAILED", "ABANDONED")


def business_totals(kpi_monthly: pd.DataFrame) -> dict:
    """Headline numbers over all months. Every customer is "new" in exactly one month, so the number of
    distinct customers is the sum of new_customers (monthly customer counts must not be summed)."""
    if kpi_monthly is None or kpi_monthly.empty:
        return {}
    orders, revenue = kpi_monthly["orders"].sum(), kpi_monthly["revenue"].sum()
    customers = kpi_monthly["new_customers"].sum()
    returning = kpi_monthly["returning_customers"].sum()
    active = kpi_monthly["customers"].sum()
    return {"revenue": float(revenue), "orders": int(orders), "customers": int(customers),
            "avg_order_value": float(revenue / orders) if orders else 0.0,
            "returning_share_pct": float(100 * returning / active) if active else 0.0}


def with_rolling(daily: pd.DataFrame, column: str = "revenue", days: int = 7) -> pd.DataFrame:
    """Daily rows sorted by date with a `<column>_<days>d_avg` trailing average (smooths day-to-day noise)."""
    out = daily.copy()
    out["date_day"] = pd.to_datetime(out["date_day"])
    out = out.sort_values("date_day")
    out[f"{column}_{days}d_avg"] = out[column].rolling(days, min_periods=1).mean().round(2)
    return out


def retention_matrix(retention: pd.DataFrame, max_months: int = 12) -> pd.DataFrame:
    """Cohort x months-since-first-order table of retention %, for a heatmap / table."""
    r = retention[retention["months_since_first"] <= max_months]
    return r.pivot(index="cohort_month", columns="months_since_first", values="retention_pct").sort_index()


def average_retention(retention: pd.DataFrame, max_months: int = 12) -> pd.DataFrame:
    """Average retention % by months since first order, weighted by cohort size (month 0 excluded)."""
    r = retention[(retention["months_since_first"] > 0) & (retention["months_since_first"] <= max_months)]
    g = r.groupby("months_since_first").agg(active=("active_customers", "sum"), size=("cohort_size", "sum"))
    g["retention_pct"] = (100 * g["active"] / g["size"]).round(2)
    return g[["retention_pct"]].reset_index()


def run_stats(flow_runs: pd.DataFrame, flow_name: str | None = "ecommerce-daily") -> dict:
    """Success/failure statistics of flow runs (RUNNING runs are not counted as finished)."""
    if flow_runs is None or flow_runs.empty:
        return {}
    runs = flow_runs if flow_name is None else flow_runs[flow_runs["flow_name"] == flow_name]
    finished = runs[runs["status"] != "RUNNING"]
    ok = finished[finished["status"] == "SUCCESS"]
    failed = finished[finished["status"].isin(FAILED_STATES)]
    latest = runs.sort_values("start_ts").iloc[-1] if len(runs) else None
    return {"runs": int(len(finished)), "succeeded": int(len(ok)), "failed": int(len(failed)),
            "success_rate_pct": float(100 * len(ok) / len(finished)) if len(finished) else 0.0,
            "median_minutes": float(ok["duration_seconds"].astype(float).median() / 60) if len(ok) else None,
            "last_status": None if latest is None else latest["status"],
            "last_start": None if latest is None else latest["start_ts"]}


def task_durations_by_run(task_runs: pd.DataFrame, flow_runs: pd.DataFrame, flow_name: str = "ecommerce-daily",
                          last: int = 15) -> pd.DataFrame:
    """Successful task durations (minutes) of the last `last` runs of a flow, long format for a stacked bar:
    columns run_label, task_name, minutes."""
    runs = flow_runs[flow_runs["flow_name"] == flow_name].sort_values("start_ts").tail(last)
    t = task_runs[(task_runs["pipeline_run_id"].isin(runs["pipeline_run_id"])) & (task_runs["status"] == "SUCCESS")]
    labels = {rid: pd.to_datetime(ts).strftime("%m-%d %H:%M")
              for rid, ts in zip(runs["pipeline_run_id"], runs["start_ts"])}
    out = t.assign(run_label=t["pipeline_run_id"].map(labels),
                   minutes=(t["duration_seconds"].astype(float) / 60).round(2))
    return out[["run_label", "task_name", "minutes"]]


def rejected_by_rule(rejected: pd.DataFrame) -> pd.DataFrame:
    """Total rows rejected per table and rule, with the latest sample keys."""
    if rejected is None or rejected.empty:
        return pd.DataFrame(columns=["table_name", "rule", "rows_rejected", "runs", "latest_samples"])
    r = rejected.sort_values("recorded_at")
    return (r.groupby(["table_name", "rule"])
            .agg(rows_rejected=("rows_rejected", "sum"), runs=("pipeline_run_id", "nunique"),
                 latest_samples=("sample_keys", "last"))
            .reset_index().sort_values("rows_rejected", ascending=False))
