"""Source (Postgres) -> Bronze (Delta).

orders  : incremental by watermark WITH an overlap window. The Bronze write is an
          insert-if-not-exists MERGE on (order_id, updated_at), so reruns, a crash
          between the write and the watermark update, overlap re-reads and backfills
          can never create duplicate rows.
backfill: --start/--end re-extract a window (by updated_at) and leave the watermark alone.
others  : full snapshot, overwritten each run.
Every row carries ingestion_ts, batch_id, source_system.
"""
import argparse
import os

import sqlalchemy as sa
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import RUN_ID, bronze_path, get_engine, get_logger, get_spark, jdbc_read
from resilience import retry
from windowing import extraction_window

log = get_logger("extract_bronze")

TABLES = {
    "orders": {"mode": "incremental", "wm_col": "updated_at", "version_keys": ["order_id", "updated_at"]},
    "customers": {"mode": "full"},
    "products": {"mode": "full"},
    "sellers": {"mode": "full"},
    "order_items": {"mode": "full"},
    "order_payments": {"mode": "full"},
}


def parse_args():
    p = argparse.ArgumentParser(description="Extract source tables to Bronze")
    p.add_argument("--start", help="backfill window start, inclusive (YYYY-MM-DD or ISO timestamp)")
    p.add_argument("--end", help="backfill window end, exclusive")
    p.add_argument("--lookback-minutes", type=int,
                   default=int(os.getenv("WATERMARK_LOOKBACK_MINUTES", "10")),
                   help="overlap window re-read on normal incremental runs")
    return p.parse_args()


@retry(attempts=3, base_delay=2)
def get_watermark(eng, table):
    with eng.connect() as c:
        return c.execute(sa.text("SELECT last_watermark FROM control.watermark WHERE table_name=:t"), {"t": table}).scalar()


@retry(attempts=3, base_delay=2)
def set_watermark(eng, table, value):
    with eng.begin() as c:
        c.execute(sa.text("UPDATE control.watermark SET last_watermark=:w, updated_at=now() WHERE table_name=:t"),
                  {"w": value, "t": table})


def insert_new_versions(spark, df, path, keys):
    """Insert-if-not-exists into Bronze. Returns the number of rows actually inserted."""
    df = df.dropDuplicates(keys)
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return df.count()
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    target = DeltaTable.forPath(spark, path)
    target.alias("t").merge(df.alias("s"), cond).whenNotMatchedInsertAll().execute()
    metrics = target.history(1).select("operationMetrics").first()[0]
    return int(metrics.get("numTargetRowsInserted", 0))


def main():
    args = parse_args()
    spark, eng = get_spark("extract_bronze"), get_engine()
    for table, cfg in TABLES.items():
        if cfg["mode"] == "incremental":
            col = cfg["wm_col"]
            wm = get_watermark(eng, table)
            lo, hi, is_backfill = extraction_window(wm, args.lookback_minutes, args.start, args.end)
            lo_s = lo.isoformat(sep=" ")
            cond = f"{col} >= TIMESTAMP '{lo_s}'"
            if hi:
                cond += f" AND {col} < TIMESTAMP '{hi.isoformat(sep=' ')}'"
            log.info("%s: %s, window: %s (watermark=%s)", table,
                     "BACKFILL" if is_backfill else "incremental", cond, wm)
            dbtable = f"(SELECT * FROM source.{table} WHERE {cond}) AS q"
        else:
            dbtable = f"source.{table}"

        df = (jdbc_read(spark, dbtable)
              .withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("batch_id", F.lit(RUN_ID))
              .withColumn("source_system", F.lit("postgres_shop")))

        if cfg["mode"] == "incremental":
            df = df.cache()
            n = df.count()
            if n == 0:
                log.info("%s: no rows in window", table)
                df.unpersist()
                continue
            inserted = insert_new_versions(spark, df, bronze_path(table), cfg["version_keys"])
            log.info("%s: extracted %d rows, inserted %d new versions (%d already in Bronze)",
                     table, n, inserted, n - inserted)
            if not is_backfill:
                new_wm = df.agg(F.max(col)).first()[0]
                if new_wm is not None and new_wm > wm:   # never move the watermark backwards
                    set_watermark(eng, table, new_wm)    # only AFTER a successful write
                    log.info("%s: watermark %s -> %s", table, wm, new_wm)
            else:
                log.info("%s: backfill, watermark left untouched", table)
            df.unpersist()
        else:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path(table))
            log.info("%s: full snapshot written", table)
    spark.stop()


if __name__ == "__main__":
    main()
