"""Bronze -> Silver (Delta): trim, dedupe, validate (bad rows -> quarantine), idempotent load.
orders uses Delta MERGE (only newer versions overwrite); other tables are full snapshots.
Quarantine is rebuilt from scratch every run, so reruns never pile up duplicate bad rows.
Each table is audited in audit.pipeline_run_log: Bronze rows read, inserted, updated, rejected (quarantined).
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from audit import audit_step, delta_last_operation, delta_version, delta_write_counts, make_batch_id
from common import bronze_path, get_engine, get_logger, get_spark, quarantine_path, silver_path
from transforms import clean_strings, dedupe_latest, split_valid_invalid

log = get_logger("transform_silver")

SPECS = {
    "orders": {"keys": ["order_id"], "order": ["updated_at", "ingestion_ts"], "mode": "merge",
               "valid": "order_purchase_timestamp IS NOT NULL"},
    "customers": {"keys": ["customer_id"], "order": ["ingestion_ts"], "mode": "overwrite", "valid": None},
    "products": {"keys": ["product_id"], "order": ["ingestion_ts"], "mode": "overwrite", "valid": None},
    "sellers": {"keys": ["seller_id"], "order": ["ingestion_ts"], "mode": "overwrite", "valid": None},
    "order_items": {"keys": ["order_id", "order_item_id"], "order": ["ingestion_ts"], "mode": "overwrite",
                    "valid": "price > 0"},
    "order_payments": {"keys": ["order_id", "payment_sequential"], "order": ["ingestion_ts"], "mode": "overwrite",
                       "valid": "payment_value >= 0"},
}


def transform_table(spark, eng, table, s):
    """Bronze -> Silver for one table inside an audit step."""
    with audit_step(table_name=f"silver.{table}", batch_id=make_batch_id(table), engine=eng) as a:
        bronze = clean_strings(spark.read.format("delta").load(bronze_path(table)))
        a.source_rows = bronze.count()
        latest = dedupe_latest(bronze, s["keys"], s["order"])
        good, bad = split_valid_invalid(latest, s["keys"], s["valid"])

        # Idempotent quarantine: full rewrite = current set of invalid rows (empty when all is clean)
        (bad.withColumn("reason", F.lit("failed_validation"))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .save(quarantine_path(table)))
        a.rejected = spark.read.format("delta").load(quarantine_path(table)).count()
        if a.rejected:
            log.warning("%s: %d invalid rows in quarantine", table, a.rejected)

        path = silver_path(table)
        before = delta_version(spark, path)
        if s["mode"] == "merge" and before is not None:
            cond = " AND ".join(f"t.{k} = s.{k}" for k in s["keys"])
            # Strictly newer versions only: Bronze keys versions on (key, updated_at), so an equal updated_at
            # is the same version. With >= every rerun rewrote (and counted as updated) every row.
            (DeltaTable.forPath(spark, path).alias("t")
                .merge(good.alias("s"), cond)
                .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
                .whenNotMatchedInsertAll()
                .execute())
        else:
            good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        a.inserted, a.updated, _, version = delta_write_counts(spark, path, before)
        log.info("%s: read %d, inserted %d, updated %d, rejected %d (silver version %d)",
                 table, a.source_rows, a.inserted, a.updated, a.rejected, version)
        a.lineage(f"delta:bronze/{table}", f"delta:silver/{table}", a.inserted + a.updated, version,
                  f"mode={s['mode']}")
        if a.rejected:
            a.lineage(f"delta:bronze/{table}", f"delta:silver/_quarantine/{table}", a.rejected,
                      delta_last_operation(spark, quarantine_path(table))[0], "failed_validation")


def main():
    spark, eng = get_spark("transform_silver"), get_engine()
    try:
        for table, s in SPECS.items():
            transform_table(spark, eng, table, s)
    finally:
        spark.stop()
        eng.dispose()


if __name__ == "__main__":
    main()
