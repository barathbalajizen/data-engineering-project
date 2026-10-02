"""Bronze -> Silver (Delta): trim, dedupe, validate (bad rows -> quarantine), idempotent load.
orders uses Delta MERGE (only newer versions overwrite); other tables are full snapshots.
Quarantine is rebuilt from scratch every run, so reruns never pile up duplicate bad rows.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import bronze_path, get_logger, get_spark, quarantine_path, silver_path
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


def main():
    spark = get_spark("transform_silver")
    for table, s in SPECS.items():
        bronze = clean_strings(spark.read.format("delta").load(bronze_path(table)))
        latest = dedupe_latest(bronze, s["keys"], s["order"])
        good, bad = split_valid_invalid(latest, s["keys"], s["valid"])

        # Idempotent quarantine: full rewrite = current set of invalid rows (empty when all is clean)
        (bad.withColumn("reason", F.lit("failed_validation"))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .save(quarantine_path(table)))
        n_bad = spark.read.format("delta").load(quarantine_path(table)).count()
        if n_bad:
            log.warning("%s: %d invalid rows in quarantine", table, n_bad)

        path = silver_path(table)
        if s["mode"] == "merge" and DeltaTable.isDeltaTable(spark, path):
            cond = " AND ".join(f"t.{k} = s.{k}" for k in s["keys"])
            (DeltaTable.forPath(spark, path).alias("t")
                .merge(good.alias("s"), cond)
                .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
                .whenNotMatchedInsertAll()
                .execute())
        else:
            good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        log.info("%s: silver rows = %d", table, spark.read.format("delta").load(path).count())
    spark.stop()


if __name__ == "__main__":
    main()
