"""Source (Postgres) -> Bronze (Delta Lake): copy the source rows into the lake, unchanged, plus metadata.

Bronze is the raw history. Nothing is cleaned or deleted here (that is Silver's job).

Three ways of extracting (TABLES below):
  orders: INCREMENTAL, with a watermark.
    - control.watermark stores the newest updated_at already extracted, e.g. 2018-03-15 10:00.
    - The next run reads orders with updated_at >= watermark - 10 minutes. These 10 minutes are the OVERLAP
      window (WATERMARK_LOOKBACK_MINUTES). Why: a slow source transaction can commit a row a little later
      than other rows, with an updated_at older than the watermark. Without the overlap it would be skipped
      forever.
    - The overlap reads some rows twice. They are not duplicated: Bronze is written with an
      insert-if-not-exists MERGE on (order_id, updated_at), so a version already in Bronze is skipped. The
      same makes reruns, retries and backfills safe.
    - A changed order is a NEW row (new updated_at). Bronze keeps every version; Silver picks the latest.
    - The watermark moves forward only AFTER the write succeeded, and never backwards (checkpoints.py).
  backfill (--start/--end): re-extract the orders whose updated_at is in [start, end), e.g. to restore lost
    data. The watermark is not touched.
  other tables: FULL SNAPSHOT. They are small: read in full and overwritten every run.

Metadata columns added to every row:
  ingestion_ts     when the row was written to Bronze
  batch_id         one id per table per attempt, e.g. orders-20261003T101500-1a2b3c4d
  source_system    where it came from: 'postgres_shop'
  pipeline_run_id  the Prefect flow run that loaded it
  source_table     e.g. 'source.orders'

Before writing, the source columns are compared with the Bronze table (schema drift, schema_drift.py): a new
column is added, a narrower type is converted (int -> bigint), and a removed column or any other type change
stops the table without writing it. Every change is recorded in audit.schema_changes.

Delta settings: the version history is kept for 30 days and old data files for at least 7 (time travel); orders
has Change Data Feed switched on, so Silver can read only the rows that changed.
Each table is logged in audit.pipeline_run_log and audit.lineage.
"""
import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from audit import audit_step, delta_version, delta_write_counts, make_batch_id
from checkpoints import advance_watermark, get_watermark
from common import RUN_ID, bronze_path, get_engine, get_logger, get_spark, jdbc_read
from delta_utils import align_to_target, delta_schema, ensure_table_properties, schema_evolution
from schema_drift import compare_schemas, enforce, schema_dict
from windowing import extraction_window

log = get_logger("extract_bronze")

# cdf: Change Data Feed. Only on incremental tables: on a full overwrite every row would show up as a
# delete + insert each run, which costs storage and says nothing about what really changed.
TABLES = {
    "orders": {"mode": "incremental", "wm_col": "updated_at", "version_keys": ["order_id", "updated_at"],
               "cdf": True},
    "customers": {"mode": "full"},
    "products": {"mode": "full"},
    "sellers": {"mode": "full"},
    "order_items": {"mode": "full"},
    "order_payments": {"mode": "full"},
}


# Time travel: old table versions stay readable for 30 days (logRetention). VACUUM only deletes data files that
# have been unused for at least 7 days (deletedFileRetention).
RETENTION_PROPS = {
    "delta.logRetentionDuration": os.getenv("BRONZE_LOG_RETENTION", "interval 30 days"),
    "delta.deletedFileRetentionDuration": os.getenv("BRONZE_FILE_RETENTION", "interval 7 days"),
}
ALLOW_REMOVED = os.getenv("SCHEMA_DRIFT_ALLOW_REMOVED_COLUMNS", "false").lower() == "true"


def table_props(cfg):
    return {**RETENTION_PROPS, **({"delta.enableChangeDataFeed": "true"} if cfg.get("cdf") else {})}


def parse_args():
    p = argparse.ArgumentParser(description="Extract source tables to Bronze")
    p.add_argument("--start", help="backfill window start, inclusive (YYYY-MM-DD or ISO timestamp)")
    p.add_argument("--end", help="backfill window end, exclusive")
    p.add_argument("--lookback-minutes", type=int,
                   default=int(os.getenv("WATERMARK_LOOKBACK_MINUTES", "10")),
                   help="overlap window re-read on normal incremental runs")
    return p.parse_args()


def check_schema(spark, a, table, src_df):
    """Compare the source columns with the Bronze table (schema_drift.py), record any change in
    audit.schema_changes, and stop the table if the change is not allowed.
    Returns (source DataFrame with allowed conversions applied, Bronze schema or None if no table yet)."""
    existing = delta_schema(spark, bronze_path(table))
    report = compare_schemas(existing, schema_dict(src_df.schema))
    if report.has_changes:
        log.warning("%s: schema drift detected: %s", table, report.summary())
        a.schema_changes(f"bronze.{table}", report.changes(ALLOW_REMOVED))   # recorded before any failure
    enforce(report, f"bronze.{table}", ALLOW_REMOVED)
    return align_to_target(src_df, report, existing or {}, ALLOW_REMOVED), existing


def write_bronze(spark, df, path, cfg, existing):
    """Write one table to Bronze. Returns (rows inserted, Delta version written).

    First run: create the table.
    Incremental (orders): MERGE that only inserts rows whose (order_id, updated_at) is not in Bronze yet.
    Full snapshot: overwrite the table.
    New columns are added to the table only when the schema check above has accepted them.
    """
    evolve = existing is not None and bool({c.lower() for c in df.columns} - set(existing))
    if existing is not None:
        ensure_table_properties(spark, path, table_props(cfg))   # before the write, so CDF covers it
    before = delta_version(spark, path)
    if existing is None:
        df.write.format("delta").mode("overwrite").save(path)
    elif cfg["mode"] == "incremental":
        df = df.dropDuplicates(cfg["version_keys"])
        cond = " AND ".join(f"t.{k} = s.{k}" for k in cfg["version_keys"])
        with schema_evolution(spark, evolve):
            DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond).whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").option("mergeSchema", str(evolve).lower()).save(path)
    inserted, _, _, version = delta_write_counts(spark, path, before)
    if existing is None:
        ensure_table_properties(spark, path, table_props(cfg))   # new table: set properties once
    if evolve:
        log.info("%s: schema evolved, new column(s): %s", path,
                 sorted({c.lower() for c in df.columns} - set(existing)))
    return inserted, version


def extract_table(spark, eng, args, table, cfg):
    """Extract one source table into Bronze inside an audit step."""
    batch_id = make_batch_id(table)
    # 1. Decide what to read: orders from the watermark minus the overlap (or the backfill window);
    #    the other tables in full
    with audit_step(table_name=f"bronze.{table}", batch_id=batch_id, engine=eng) as a:
        window = "full snapshot"
        if cfg["mode"] == "incremental":
            col = cfg["wm_col"]
            wm = get_watermark(eng, table)
            lo, hi, is_backfill = extraction_window(wm, args.lookback_minutes, args.start, args.end)
            cond = f"{col} >= TIMESTAMP '{lo.isoformat(sep=' ')}'"
            if hi:
                cond += f" AND {col} < TIMESTAMP '{hi.isoformat(sep=' ')}'"
            window = ("BACKFILL " if is_backfill else "incremental ") + cond
            log.info("%s: %s (watermark=%s, batch=%s)", table, window, wm, batch_id)
            dbtable = f"(SELECT * FROM source.{table} WHERE {cond}) AS q"
        else:
            dbtable = f"source.{table}"

        # 2. Read from Postgres, check the schema against Bronze, add the metadata columns
        src_df, existing = check_schema(spark, a, table, jdbc_read(spark, dbtable))
        df = (src_df
              .withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("source_system", F.lit("postgres_shop"))
              .withColumn("pipeline_run_id", F.lit(RUN_ID))
              .withColumn("source_table", F.lit(f"source.{table}")))
        a.rejected = a.updated = 0

        # 3. Write to Bronze. Orders: then move the watermark (normal runs only, never on a backfill)
        if cfg["mode"] == "incremental":
            df = df.cache()
            n = a.source_rows = df.count()
            if n == 0:
                log.info("%s: no rows in window", table)
                a.inserted = 0
                df.unpersist()
                return
            a.inserted, version = write_bronze(spark, df, bronze_path(table), cfg, existing)
            log.info("%s: extracted %d rows, inserted %d new versions (%d already in Bronze)",
                     table, n, a.inserted, n - a.inserted)
            if not is_backfill:
                new_wm = df.agg(F.max(col)).first()[0]
                # only AFTER a successful write; advance_watermark never moves it backwards
                if new_wm is not None and advance_watermark(eng, table, new_wm, RUN_ID):
                    log.info("%s: watermark %s -> %s", table, wm, new_wm)
            else:
                log.info("%s: backfill, watermark left untouched", table)
            df.unpersist()
        else:
            a.inserted, version = write_bronze(spark, df, bronze_path(table), cfg, existing)
            a.source_rows = a.inserted
            log.info("%s: full snapshot written, %d rows", table, a.inserted)
        a.lineage(f"postgres:source.{table}", f"delta:bronze/{table}", a.inserted, version, window)


def main():
    args = parse_args()
    spark, eng = get_spark("extract_bronze"), get_engine()
    try:
        for table, cfg in TABLES.items():
            extract_table(spark, eng, args, table, cfg)
    finally:
        spark.stop()
        eng.dispose()


if __name__ == "__main__":
    main()
