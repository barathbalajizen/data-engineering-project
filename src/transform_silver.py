"""Bronze -> Silver: clean, validate and deduplicate every table, and set bad rows aside with the reason.

Steps for every table:
  1. Read the Bronze rows to process.
  2. Trim spaces in every text column.
  3. Check every row against the table's rules (SPECS below; rule helpers in validation.py). A row that breaks
     a rule goes to the quarantine (lake/silver/_quarantine/<table>) with the names of ALL the rules it broke,
     e.g. failed_rules = [not_null:customer_id, accepted:order_status].
  4. Keep one row per key: the latest VALID version (dedupe_latest). Validating BEFORE deduplicating matters:
     if the newest version of an order is broken, Silver keeps the previous good version instead of losing it.
  5. Write Silver, and record rows read / inserted / updated / rejected and lineage in the audit tables.

Two ways of loading:
  orders: INCREMENTAL (process_incremental). Bronze orders grows every day, so only what changed is read.
    - Every write to a Delta table creates a new numbered version (0, 1, 2 ...). control.silver_checkpoint
      stores the last Bronze version already merged into Silver.
    - Delta Change Data Feed returns only the rows added in the Bronze versions after the checkpoint.
    - Those rows are MERGEd into Silver by order_id:
          order_id not in Silver yet            -> inserted
          newer updated_at than the Silver row  -> replaces it
          same updated_at                       -> nothing (already there)
          OLDER updated_at (late-arriving)      -> ignored; Silver keeps the newer data (logged as late_ignored)
    - Nothing new in Bronze -> the table is skipped. When reading only the changes is impossible or unsafe
      (first run, change data expired, --full-refresh ...) all of Bronze is read instead; the MERGE gives the
      same result. decide_mode in incremental.py lists every case.
    - The quarantine is a log that only grows: a rejected version is added once (insert-if-not-exists on
      order_id + updated_at), so running again never duplicates it.
    - The checkpoint moves only AFTER Silver and the quarantine are written. If the run fails halfway, the
      next run reads the same changes again, which is safe because the MERGE is idempotent.
  other tables: FULL SNAPSHOT (process_full). They are small: re-read in full and overwritten every run.
    Their quarantine is overwritten too, so it always shows the rows that are invalid right now.
"""
import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from audit import audit_step, delta_version, delta_write_counts, make_batch_id
from common import RUN_ID, bronze_path, get_engine, get_logger, get_spark, quarantine_path, silver_path, sized_for_write
from delta_utils import cdf_enabled_since, is_delta, read_changes, schema_evolution
from incremental import FULL, INCREMENTAL, SKIP, PgCheckpointStore, decide_mode
from transforms import classify_versions, clean_strings, dedupe_latest
from validation import accepted, apply_rules, castable, check, not_null, rule_failure_counts, rule_samples

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
    """Steps 2-4 for one batch of rows. Returns:
    latest      the latest valid version per key (what goes to Silver)
    invalid     the rows that broke a rule, with failed_rules and reason (what goes to the quarantine)
    failures    {rule name: number of rows that broke it}
    duplicates  how many older valid versions were dropped by the deduplication
    """
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
    """Write the latest valid versions into Silver: create the table on the first run, MERGE on the key after
    (insert new keys, update only when the incoming version is newer)."""
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
    """Silver for orders: process only the Bronze changes since the checkpoint (see the module docstring)."""
    # 1. Decide: skip, incremental or full (every case is in decide_mode, incremental.py)
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
    # 2. Read the Bronze rows: the versions after the checkpoint (incremental) or the whole table (full)
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

    # 3. Validate and deduplicate; count how each version compares with Silver (new / newer / same / late)
    latest, invalid, failures, duplicates = validate(batch, s)
    target = spark.read.format("delta").load(path) if is_delta(spark, path) else None
    kinds = classify_versions(latest, target, s["keys"], s["version_col"])

    # 4. Write the quarantine, then Silver, then move the checkpoint (last, so a failure means a safe re-read)
    a.rejected = invalid.count()
    merge_quarantine_log(spark, invalid, table, s)
    before = delta_version(spark, path)
    merge_silver(spark, latest, path, s)
    a.inserted, a.updated, _, version = delta_write_counts(spark, path, before)
    store.set(table, bronze_v, mode)                 # only after Silver and quarantine are written

    # 5. Report. Orders whose newest version was rejected: Silver keeps their last VALID version
    #    (e.g. version 2 has an unknown status -> quarantined; Silver still shows version 1)
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
        a.rejections(f"silver.{table}", failures, rule_samples(invalid, s["keys"]))
    for df in (batch, latest, invalid):
        df.unpersist()


# ------------------------------------------------------------- full snapshot
def process_full(spark, a, table, s):
    """Silver for a small table: read all of Bronze, validate, then overwrite Silver and its quarantine."""
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
        a.rejections(f"silver.{table}", failures, rule_samples(invalid, s["keys"]))
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
