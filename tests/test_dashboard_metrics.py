"""Unit tests for the dashboard's pandas helpers (dashboard/metrics.py)."""
import sys
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
import metrics as m  # noqa: E402


def test_business_totals_do_not_double_count_customers():
    kpi = pd.DataFrame({"month": ["2026-01", "2026-02"], "orders": [10, 30], "revenue": [1000.0, 2000.0],
                        "customers": [8, 20], "new_customers": [8, 15], "returning_customers": [0, 5]})
    t = m.business_totals(kpi)
    assert t["orders"] == 40 and t["revenue"] == 3000.0 and t["avg_order_value"] == 75.0
    assert t["customers"] == 23                      # 8 + 15 new, not 8 + 20 monthly actives
    assert t["returning_share_pct"] == pytest.approx(100 * 5 / 28)
    assert m.business_totals(pd.DataFrame()) == {}


def test_rolling_average_is_sorted_by_date():
    daily = pd.DataFrame({"date_day": ["2026-01-03", "2026-01-01", "2026-01-02"], "revenue": [30.0, 10.0, 20.0]})
    out = m.with_rolling(daily, days=2)
    assert list(out["revenue_2d_avg"]) == [10.0, 15.0, 25.0]


def _retention():
    return pd.DataFrame({"cohort_month": ["2026-01"] * 3 + ["2026-02"] * 2,
                         "months_since_first": [0, 1, 2, 0, 1],
                         "cohort_size": [100, 100, 100, 50, 50],
                         "active_customers": [100, 10, 5, 50, 10],
                         "retention_pct": [100.0, 10.0, 5.0, 100.0, 20.0]})


def test_retention_matrix():
    mat = m.retention_matrix(_retention())
    assert list(mat.index) == ["2026-01", "2026-02"] and mat.loc["2026-02", 1] == 20.0
    assert pd.isna(mat.loc["2026-02", 2])


def test_average_retention_is_weighted_by_cohort_size():
    avg = m.average_retention(_retention())
    assert list(avg["months_since_first"]) == [1, 2]
    assert avg.loc[avg["months_since_first"] == 1, "retention_pct"].item() == pytest.approx(100 * 20 / 150, abs=0.01)


def _runs():
    return pd.DataFrame({
        "pipeline_run_id": ["r1", "r2", "r3", "r4", "r5"],
        "flow_name": ["ecommerce-daily"] * 4 + ["ecommerce-setup"],
        "status": ["SUCCESS", "FAILED", "SUCCESS", "RUNNING", "SUCCESS"],
        "start_ts": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]),
        "duration_seconds": [600, 60, 1200, None, 30]})


def test_run_stats():
    s = m.run_stats(_runs())
    assert (s["runs"], s["succeeded"], s["failed"]) == (3, 2, 1)       # RUNNING not finished; setup excluded
    assert s["success_rate_pct"] == pytest.approx(66.67, abs=0.01)
    assert s["median_minutes"] == 15.0 and s["last_status"] == "RUNNING"
    assert m.run_stats(_runs(), flow_name=None)["runs"] == 4


def test_task_durations_by_run_uses_successful_attempts_only():
    tasks = pd.DataFrame({"pipeline_run_id": ["r1", "r1", "r3", "r3"],
                          "task_name": ["extract-bronze", "dbt-build", "extract-bronze", "extract-bronze"],
                          "status": ["SUCCESS", "SUCCESS", "FAILED", "SUCCESS"],
                          "duration_seconds": [120, 30, 5, 90]})
    out = m.task_durations_by_run(tasks, _runs())
    assert len(out) == 3 and set(out["task_name"]) == {"extract-bronze", "dbt-build"}
    assert out.loc[out["run_label"] == "01-03 00:00", "minutes"].item() == 1.5


def test_rejected_by_rule():
    rej = pd.DataFrame({"recorded_at": ["2026-01-01", "2026-01-02", "2026-01-02"],
                        "pipeline_run_id": ["r1", "r2", "r2"],
                        "table_name": ["silver.orders"] * 3,
                        "rule": ["accepted:order_status", "accepted:order_status", "not_null:order_id"],
                        "rows_rejected": [2, 3, 1], "sample_keys": ["order_id=a", "order_id=b", "order_id=c"]})
    out = m.rejected_by_rule(rej)
    top = out.iloc[0]
    assert (top["rule"], top["rows_rejected"], top["runs"], top["latest_samples"]) == \
        ("accepted:order_status", 5, 2, "order_id=b")
    assert list(m.rejected_by_rule(pd.DataFrame()).columns)[:2] == ["table_name", "rule"]
