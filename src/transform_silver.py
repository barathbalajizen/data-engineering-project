"""Bronze -> Silver (Delta): trim, validate, quarantine, deduplicate, idempotent load.

Per table:  trim strings -> validation rules (validation.py) -> invalid rows to quarantine with the rules they
failed -> latest VALID version per key -> Silver. Validating before deduplicating means a bad newer version
never hides the last good one.

orders (incremental): reads only the Bronze commits since the last checkpoint (control.silver_checkpoint)
    through Delta Change Data Feed, and MERGEs them: new keys are inserted, strictly newer versions update,
    equal versions are no-ops and LATE-ARRIVING older versions are ignored (counted in the lineage detail).
    Nothing new in Bronze -> the table is skipped (no rewrite). Falls back to a full pass when there is no
    checkpoint, CDF data is missing (retention, table recreated) or with --full-refresh /
    SILVER_FULL_REFRESH=true. The quarantine is a log of rejected versions, merged insert-if-not-exists
    on (key, updated_at), so reruns never duplicate it. The checkpoint moves only after both writes succeed.
others (full snapshot): small tables re-read in full and overwritten; quarantine = current invalid rows.

Each table is audited (rows read, inserted, updated, rejected) with lineage, including the Bronze versions read.
"""
import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from audit import audit_step, delta_version, delta_write_counts, make_batch_id
from common import (RUN_ID, bronze_path, get_engine, get_logger, get_spark, quarantine_path, silver_path,
                    sized_for_write)
from delta_utils import cdf_enabled_since, is_delta, read_changes, schema_evolution
from incremental import FULL, INCREMENTAL, SKIP, PgCheckpointStore, decide_mode
from transforms import classify_versions, clean_strings, dedupe_latest
from validation import accepted, apply_rules, castable, check, not_null, rule_failure_counts

log = get_logger("transform_silver")

ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped", "delivered", "unavailable", "canceled"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

SPECS = {
    "orders": {
        "keys": ["order_id"], "version_col": "updated_at", "order": ["updated_at", "ingestion_ts"],
        "mode": "incremental",
        "rules": not_null("order_id", "customer_id", "order_purchase_timestamp", "updated_at") + [
            accepted("order_status", ORDER_STATUSES),
            castable("order_purchase_timestamp", "timestamp"),
            check("delivered_after_purchase", "order_delivered_customer_date IS NULL "
                                              "OR order_delivered_customer_date >= order_purchase_timestamp"),
        ]},
    "customers": {
        "keys": ["customer_id"], "order": ["ingestion_ts"], "mode": "full",
        "rules": not_null("customer_id") + [
            check("state_code_2_chars", "customer_state IS NULL OR length(customer_state) = 2")]},
    "products": {
        "keys": ["product_id"], "order": ["ingestion_ts"], "mode": "full",
        "rules": not_null("product_id") + [
            castable("product_weight_g", "double"),
            check("weight_not_negative", "product_weight_g IS NULL OR product_weight_g >= 0")]},
    "sellers": {
        "keys": ["seller_id"], "order": ["ingestion_ts"], "mode": "full",
        "rules": not_null("seller_id")},
    "order_items": {
        "keys": ["order_id", "order_item_id"], "order": ["ingestion_ts"], "mode": "full",
        "rules": not_null("order_id", "order_item_id", "product_id", "seller_id") + [
            castable("price", "double"),
            check("price_positive", "price > 0"),
            check("freight_not_negative", "freight_value IS NULL OR freight_value >= 0")]},
    "order_payments": {
        "keys": ["order_id", "payment_sequential"], "order": ["ingestion_ts"], "mode": "full",
        "rules": not_null("order_id", "payment_sequential") + [
            accepted("payment_type", PAYMENT_TYPES),
            castable("payment_value", "double"),
            check("payment_value_not_negative", "payment_value >= 0")]},
}


def parse_args():
    p = argparse.ArgumentParser(description="Bronze -> Silver")
    p.add_argument("--full-refresh", action="store_true",
                   default=os.getenv("SILVER_FULL_REFRESH", "false").lower() == "true",
                   help="ignore the checkpoint and re-read all of Bronze (still an idempotent MERGE)")
    return p.parse_args()


def validate(df, s):
    """(latest valid version per key, invalid rows with failed_rules/reason, rule failure counts, duplicates)."""
    valid, invalid = apply_rules(clean_strings(df), s["rules"])
    valid = valid.cache()
    latest = dedupe_latest(valid, s["keys"], s["order"]).cache()
    duplicates = valid.count() - latest.count()
    valid.unpersist()
    invalid = invalid.withColumn("quarantined_at", F.current_timestamp()).cache()
    return latest, invalid, rule_failure_counts(invalid), duplicates


# --------------------------------------------------------------- incremental
def read_bronze(spark, table, mode, start, end):
    """Bronze rows to process: the whole table at version `end` (full), or the rows inserted/updated in
    versions start..end (incremental, Change Data Feed). Pinning `end` makes the checkpoint exact."""
    path = bronze_path(table)
    if mode == FULL:
        return spark.read.format("delta").option("versionAsOf", end).load(path)
    changes = read_changes(spark, path, start, end)
    # Deletes in Bronze (e.g. simulated data loss) are not propagated: Silver keeps the last valid version
    return changes.filter(F.col("_change_type").isin("insert", "update_postimage")).drop(*CDF_COLUMNS)


def merge_quarantine_log(spark, invalid, table, s):
    """Insert-if-not-exists rejected versions on (key, version), so reruns never duplicate the log."""
    path = quarantine_path(table)
    keys = s["keys"] + [s["version_col"]]
    if not is_delta(spark, path):
        sized_for_write(invalid, invalid.count()).write.format("delta").mode("overwrite").save(path)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    with schema_evolution(spark):   # adds failed_rules to quarantine tables created before rule-based checks
        DeltaTable.forPath(spark, path).alias("t").merge(
            invalid.dropDuplicates(keys).alias("s"), cond).whenNotMatchedInsertAll().execute()


def merge_silver(spark, latest, path, s):
    if not is_delta(spark, path):
        sized_for_write(latest, latest.count()).write.format("delta").mode("overwrite").save(path)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in s["keys"])
    vc = s["version_col"]
    # Strictly newer versions only: an equal version is the same row, an older one is a late arrival.
    # Schema evolution is on because Bronze already gated the schema (extract_bronze.check_schema).
    with schema_evolution(spark):
        (DeltaTable.forPath(spark, path).alias("t")
            .merge(latest.alias("s"), cond)
            .whenMatchedUpdateAll(condition=f"s.{vc} > t.{vc}")
            .whenNotMatchedInsertAll()
            .execute())


def process_incremental(spark, a, table, s, store, force_full):
    path, bpath = silver_path(table), bronze_path(table)
    bronze_v = delta_version(spark, bpath)
    checkpoint, target_exists = store.get(table), is_delta(spark, path)
    # Nothing new in Bronze: skip without scanning the table history for the CDF start version
    nothing_new = not force_full and target_exists and checkpoint is not None and bronze_v == checkpoint
    cdf_since = None if nothing_new else cdf_enabled_since(spark, bpath)
    mode, why = decide_mode(checkpoint, bronze_v, cdf_since, target_exists, force_full)
    log.info("%s: %s (%s)", table, mode.upper(), why)
    a.source_rows = a.inserted = a.updated = a.rejected = 0
    if mode == SKIP:
        return
    start = checkpoint + 1 if mode == INCREMENTAL else 0
    try:
        batch = read_bronze(spark, table, mode, start, bronze_v).cache()
        a.source_rows = batch.count()
    except Exception as exc:   # CDF files removed by retention/VACUUM etc.: a full pass is always correct
        if mode != INCREMENTAL:
            raise
        log.warning("%s: change data for versions %d..%d unreadable (%s); falling back to a full pass",
                    table, start, bronze_v, str(exc).splitlines()[0][:200])
        mode, start = FULL, 0
        batch = read_bronze(spark, table, mode, start, bronze_v).cache()
        a.source_rows = batch.count()

    latest, invalid, failures, duplicates = validate(batch, s)
    target = spark.read.format("delta").load(path) if is_delta(spark, path) else None
    kinds = classify_versions(latest, target, s["keys"], s["version_col"])

    a.rejected = invalid.count()
    merge_quarantine_log(spark, invalid, table, s)
    before = delta_version(spark, path)
    merge_silver(spark, latest, path, s)
    a.inserted, a.updated, _, version = delta_write_counts(spark, path, before)
    store.set(table, bronze_v, mode)                 # only after Silver and quarantine are written

    # Rejected versions newer than what Silver now holds: Silver keeps the last VALID version (stale vs source)
    superseded = classify_versions(dedupe_latest(invalid, s["keys"], s["order"]),
                                   spark.read.format("delta").load(path), s["keys"], s["version_col"])["newer"]

    detail = (f"mode={mode} bronze_versions={start}..{bronze_v} new={kinds['new']} newer={kinds['newer']} "
              f"same={kinds['same']} late_ignored={kinds['late']} duplicates={duplicates} "
              f"superseded_by_invalid={superseded}")
    log.info("%s: read %d, inserted %d, updated %d, rejected %d | %s", table, a.source_rows, a.inserted,
             a.updated, a.rejected, detail)
    if kinds["late"]:
        log.warning("%s: %d late-arriving older version(s) ignored (Silver already has newer data)",
                    table, kinds["late"])
    if superseded:
        log.warning("%s: %d order(s) have a newer version that failed validation; Silver keeps the last valid "
                    "version (see quarantine)", table, superseded)
    a.lineage(f"delta:bronze/{table}", f"delta:silver/{table}", a.inserted + a.updated, version, detail)
    if a.rejected:
        log.warning("%s: %d row(s) quarantined: %s", table, a.rejected, failures)
        a.lineage(f"delta:bronze/{table}", f"delta:silver/_quarantine/{table}", a.rejected,
                  delta_version(spark, quarantine_path(table)), f"rules={failures}")
    for df in (batch, latest, invalid):
        df.unpersist()


# ------------------------------------------------------------- full snapshot
def process_full(spark, a, table, s):
    path = silver_path(table)
    bronze_v = delta_version(spark, bronze_path(table))
    bronze = read_bronze(spark, table, FULL, 0, bronze_v).cache()
    a.source_rows = bronze.count()
    latest, invalid, failures, duplicates = validate(bronze, s)

    # Quarantine = current set of invalid rows (full rewrite, so reruns never pile up)
    a.rejected = invalid.count()
    (sized_for_write(invalid, a.rejected).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .save(quarantine_path(table)))
    before = delta_version(spark, path)
    (sized_for_write(latest, latest.count()).write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(path))
    a.inserted, a.updated, _, version = delta_write_counts(spark, path, before)
    log.info("%s: read %d, written %d, rejected %d, duplicates %d (silver version %d)",
             table, a.source_rows, a.inserted, a.rejected, duplicates, version)
    a.lineage(f"delta:bronze/{table}@v{bronze_v}", f"delta:silver/{table}", a.inserted, version,
              f"mode=full duplicates={duplicates}")
    if a.rejected:
        log.warning("%s: %d row(s) quarantined: %s", table, a.rejected, failures)
        a.lineage(f"delta:bronze/{table}@v{bronze_v}", f"delta:silver/_quarantine/{table}", a.rejected,
                  delta_version(spark, quarantine_path(table)), f"rules={failures}")
    for df in (bronze, latest, invalid):
        df.unpersist()


def transform_table(spark, eng, table, s, store, force_full=False):
    """Bronze -> Silver for one table inside an audit step."""
    with audit_step(table_name=f"silver.{table}", batch_id=make_batch_id(table), engine=eng) as a:
        if s["mode"] == "incremental":
            process_incremental(spark, a, table, s, store, force_full)
        else:
            process_full(spark, a, table, s)


def main():
    args = parse_args()
    spark, eng = get_spark("transform_silver"), get_engine()
    store = PgCheckpointStore(eng, RUN_ID)
    try:
        for table, s in SPECS.items():
            transform_table(spark, eng, table, s, store, args.full_refresh)
    finally:
        spark.stop()
        eng.dispose()


if __name__ == "__main__":
    main()
