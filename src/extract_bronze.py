"""Source (Postgres) -> Bronze (Delta).

orders  : incremental by watermark WITH an overlap window. The Bronze write is an
          insert-if-not-exists MERGE on (order_id, updated_at), so reruns, a crash
          between the write and the watermark update, overlap re-reads and backfills
          can never create duplicate rows.
backfill: --start/--end re-extract a window (by updated_at) and leave the watermark alone.
others  : full snapshot, overwritten each run.
Every row carries ingestion_ts, batch_id (unique per table per run attempt), source_system, pipeline_run_id
and source_table. Each table is audited in audit.pipeline_run_log with lineage in audit.lineage.

Schema drift: the source schema is compared with the accepted Bronze schema before writing (schema_drift.py).
New columns evolve the table, narrower types are cast, removed columns and other type changes fail the table
(SCHEMA_DRIFT_ALLOW_REMOVED_COLUMNS=true fills removed columns with NULL instead). Every change is recorded
in audit.schema_changes. Tables keep a known time-travel window (retention properties) and orders has Change
Data Feed enabled so downstream steps can read only what changed.
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


# Time-travel window: versions stay readable for logRetention; VACUUM keeps old files for deletedFileRetention
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
    """Compare the source schema with the accepted Bronze schema, record and enforce the drift policy.
    Returns (aligned source DataFrame, existing Bronze schema or None)."""
    existing = delta_schema(spark, bronze_path(table))
    report = compare_schemas(existing, schema_dict(src_df.schema))
    if report.has_changes:
        log.warning("%s: schema drift detected: %s", table, report.summary())
        a.schema_changes(f"bronze.{table}", report.changes(ALLOW_REMOVED))   # recorded before any failure
    enforce(report, f"bronze.{table}", ALLOW_REMOVED)
    return align_to_target(src_df, report, existing or {}, ALLOW_REMOVED), existing


def write_bronze(spark, df, path, cfg, existing):
    """Write one table to Bronze. Returns (rows inserted, Delta version written).

    incremental: insert-if-not-exists MERGE on the version keys; full: overwrite.
    Schema evolution is switched on only when the (already accepted) DataFrame has columns the table lacks.
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

        src_df, existing = check_schema(spark, a, table, jdbc_read(spark, dbtable))
        df = (src_df
              .withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("source_system", F.lit("postgres_shop"))
              .withColumn("pipeline_run_id", F.lit(RUN_ID))
              .withColumn("source_table", F.lit(f"source.{table}")))
        a.rejected = a.updated = 0

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
