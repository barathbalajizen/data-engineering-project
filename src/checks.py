"""Data quality + reconciliation checks. Results go to audit.dq_log.
Any CRITICAL failure exits non-zero so the orchestrator marks the task failed.

Source -> Silver reconciliation is key- and version-level (transforms.reconcile_versions): every source order
must be in Silver or in the quarantine, and a Silver version older than the source must be explained by a
rejected newer version.

Business metrics (revenue, payments, orders) are reconciled from staging to Gold exactly (critical: catches a
broken incremental model or rows dropped by a join) and from the source to Gold as a warning (quarantined
rows or source changes during the run explain differences there).

Freshness: data freshness (when new data last arrived) is dbt source freshness (dbt_build.sh). Here,
pipeline freshness: the previous successful daily run must be recent, which catches missed schedules.
Every result has a category for the scorecard (audit.dq_scorecard).
"""
import sys

import sqlalchemy as sa
from common import RUN_ID, bronze_path, get_engine, get_logger, get_spark, jdbc_read, quarantine_path, silver_path
from delta_utils import is_delta
from resilience import retry
from transforms import reconcile_versions

log = get_logger("checks")
results = []


PIPELINE_MAX_GAP_HOURS = 26   # daily schedule + slack

# Business metrics per layer (non-canceled orders, like the Gold aggregates); {s} = source or staging
LAYER_SQL = {
    "revenue": ("SELECT coalesce(sum(i.price), 0) FROM {s}.order_items i JOIN {s}.orders o USING (order_id) "
                "WHERE o.order_status <> 'canceled'"),
    "orders": ("SELECT count(DISTINCT o.order_id) FROM {s}.order_items i JOIN {s}.orders o USING (order_id) "
               "WHERE o.order_status <> 'canceled'"),
    "payments": ("SELECT coalesce(sum(p.payment_value), 0) FROM {s}.order_payments p "
                 "JOIN {s}.orders o USING (order_id)"),
}
GOLD_SQL = {
    "revenue": "SELECT coalesce(sum(revenue), 0) FROM analytics.agg_daily_sales",
    "orders": "SELECT coalesce(sum(orders), 0) FROM analytics.agg_daily_sales",
    "payments": "SELECT coalesce(sum(payment_value), 0) FROM analytics.fact_payments",
}


def record(name, ok, rows_failed=0, detail="", critical=True, category="business_rule"):
    status = "PASS" if ok else ("FAIL" if critical else "WARN")
    results.append({"run_id": RUN_ID, "name": name, "status": status, "failed": int(rows_failed), "detail": detail,
                    "source": "checks", "category": category})
    log.info("%s: %s %s", name, status, detail)


def metrics_match(a, b, tolerance=0.01):
    """True when two metric values agree within an absolute tolerance (money is summed as float)."""
    return abs(float(a) - float(b)) <= tolerance


@retry(attempts=3, base_delay=2)
def write_results(eng):
    with eng.begin() as c:
        for r in results:
            c.execute(sa.text("INSERT INTO audit.dq_log (run_id, check_name, status, rows_failed, detail, "
                              "check_source, category) VALUES (:run_id, :name, :status, :failed, :detail, "
                              ":source, :category)"), r)


def check_business_metrics(scalar):
    for metric, sql in LAYER_SQL.items():
        gold = float(scalar(GOLD_SQL[metric]))
        stg, src = float(scalar(sql.format(s="staging"))), float(scalar(sql.format(s="source")))
        record(f"recon_{metric}_staging_vs_gold", metrics_match(stg, gold), 0,
               f"staging={stg:.2f} gold={gold:.2f} diff={gold - stg:.2f}", category="reconciliation")
        record(f"recon_{metric}_source_vs_gold", metrics_match(src, gold), 0,
               f"source={src:.2f} gold={gold:.2f} diff={gold - src:.2f}", critical=False, category="reconciliation")


def check_pipeline_freshness(c):
    """Hours since the previous successful daily run (WARN when a scheduled run was missed)."""
    hours = c.execute(sa.text(
        "SELECT extract(epoch FROM (now() - max(end_ts))) / 3600 FROM audit.pipeline_run_log "
        "WHERE level = 'flow' AND task_name = 'ecommerce-daily' AND status = 'SUCCESS' "
        "AND pipeline_run_id <> :run"), {"run": RUN_ID}).scalar()
    if hours is None:
        record("pipeline_previous_success_recent", True, 0, "no previous successful daily run (first run)",
               critical=False, category="freshness")
    else:
        record("pipeline_previous_success_recent", float(hours) <= PIPELINE_MAX_GAP_HOURS, 0,
               f"hours_since_previous_success={float(hours):.1f} limit={PIPELINE_MAX_GAP_HOURS}",
               critical=False, category="freshness")


def main():
    spark, eng = get_spark("checks"), get_engine()
    bronze = spark.read.format("delta").load(bronze_path("orders"))
    dup_versions = bronze.count() - bronze.select("order_id", "updated_at").distinct().count()
    record("bronze_orders_no_duplicate_versions", dup_versions == 0, dup_versions, category="uniqueness")

    with eng.connect() as c:
        scalar = lambda sql: c.execute(sa.text(sql)).scalar()  # noqa: E731

        silver = spark.read.format("delta").load(silver_path("orders"))
        silver_n = silver.count()
        q_path = quarantine_path("orders")
        quarantine = spark.read.format("delta").load(q_path) if is_delta(spark, q_path) else None
        source = jdbc_read(spark, "(SELECT order_id, updated_at FROM source.orders) AS q")
        r = reconcile_versions(source, silver, quarantine, "order_id", "updated_at")
        record("recon_source_vs_silver_orders", r["missing"] == 0, r["missing"],
               f"source={r['source']} silver={r['in_silver']} quarantined_only={r['quarantined_only']} "
               f"missing={r['missing']}", category="reconciliation")
        record("recon_source_vs_silver_versions", r["stale_unexplained"] == 0 and r["ahead"] == 0,
               r["stale_unexplained"] + r["ahead"],
               f"stale_explained_by_quarantine={r['stale_explained']} stale_unexplained={r['stale_unexplained']} "
               f"ahead={r['ahead']}", critical=False,   # the source may change while the run is in progress
               category="reconciliation")

        dup = silver_n - silver.select("order_id").distinct().count()
        record("silver_orders_unique_key", dup == 0, dup, category="uniqueness")

        stg_n = scalar("SELECT count(*) FROM staging.orders")
        record("recon_silver_vs_staging_orders", stg_n == silver_n, abs(stg_n - silver_n),
               f"silver={silver_n} staging={stg_n}", category="reconciliation")

        null_ck = scalar("SELECT count(*) FROM analytics.fact_orders WHERE customer_key IS NULL")
        record("fact_orders_no_null_customer_key", null_ck == 0, null_ck, category="completeness")

        items_n = scalar("SELECT count(*) FROM staging.order_items")
        fact_n = scalar("SELECT count(*) FROM analytics.fact_orders")
        record("recon_items_vs_fact", items_n == fact_n, abs(items_n - fact_n), f"items={items_n} fact={fact_n}",
               critical=False, category="reconciliation")

        check_business_metrics(scalar)
        check_pipeline_freshness(c)

    write_results(eng)
    spark.stop()
    if any(r["status"] == "FAIL" for r in results):
        log.error("Critical data quality check(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
