"""Data quality + reconciliation checks. Results go to audit.dq_log.
Any CRITICAL failure exits non-zero so the orchestrator marks the task failed.
"""
import sys

import sqlalchemy as sa
from pyspark.sql import functions as F

from common import RUN_ID, bronze_path, get_engine, get_logger, get_spark, silver_path
from resilience import retry

log = get_logger("checks")
results = []


def record(name, ok, rows_failed=0, detail="", critical=True):
    status = "PASS" if ok else ("FAIL" if critical else "WARN")
    results.append({"run_id": RUN_ID, "name": name, "status": status, "failed": int(rows_failed), "detail": detail})
    log.info("%s: %s %s", name, status, detail)


@retry(attempts=3, base_delay=2)
def write_results(eng):
    with eng.begin() as c:
        for r in results:
            c.execute(sa.text("INSERT INTO audit.dq_log(run_id,check_name,status,rows_failed,detail) "
                              "VALUES (:run_id,:name,:status,:failed,:detail)"), r)


def main():
    spark, eng = get_spark("checks"), get_engine()
    bronze = spark.read.format("delta").load(bronze_path("orders"))
    dup_versions = bronze.count() - bronze.select("order_id", "updated_at").distinct().count()
    record("bronze_orders_no_duplicate_versions", dup_versions == 0, dup_versions)

    with eng.connect() as c:
        scalar = lambda sql: c.execute(sa.text(sql)).scalar()  # noqa: E731

        src_n = scalar("SELECT count(*) FROM source.orders WHERE order_id IS NOT NULL AND order_purchase_timestamp IS NOT NULL")
        silver = spark.read.format("delta").load(silver_path("orders"))
        silver_n = silver.count()
        record("recon_source_vs_silver_orders", src_n == silver_n, abs(src_n - silver_n), f"source={src_n} silver={silver_n}")

        dup = silver_n - silver.select("order_id").distinct().count()
        record("silver_orders_unique_key", dup == 0, dup)

        stg_n = scalar("SELECT count(*) FROM staging.orders")
        record("recon_silver_vs_staging_orders", stg_n == silver_n, abs(stg_n - silver_n), f"silver={silver_n} staging={stg_n}")

        null_ck = scalar("SELECT count(*) FROM analytics.fact_orders WHERE customer_key IS NULL")
        record("fact_orders_no_null_customer_key", null_ck == 0, null_ck)

        items_n = scalar("SELECT count(*) FROM staging.order_items")
        fact_n = scalar("SELECT count(*) FROM analytics.fact_orders")
        record("recon_items_vs_fact", items_n == fact_n, abs(items_n - fact_n), f"items={items_n} fact={fact_n}", critical=False)

    age_h = (spark.read.format("delta").load(bronze_path("orders"))
             .agg(((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("ingestion_ts"))) / 3600)).first()[0])
    record("bronze_orders_freshness_24h", age_h is not None and age_h <= 24, 0, f"age_hours={age_h:.1f}", critical=False)

    write_results(eng)
    spark.stop()
    if any(r["status"] == "FAIL" for r in results):
        log.error("Critical data quality check(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
