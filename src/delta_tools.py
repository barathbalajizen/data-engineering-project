"""Read-only Delta Lake inspection: history, time travel and Change Data Feed.

  python delta_tools.py history  --table bronze/orders [--limit 10]
  python delta_tools.py as-of    --table bronze/orders --version 3          (or --timestamp "2026-10-03 05:00")
  python delta_tools.py changes  --table bronze/orders --from-version 4 [--to-version 6]

as-of compares the old version with the current table: row counts and keys added / missing since then.
changes summarises the CDF rows per commit and change type (only for tables with CDF enabled, see
extract_bronze.TABLES). Nothing is ever written.
"""
import argparse

from pyspark.sql import functions as F

from common import LAKE, get_logger, get_spark
from delta_utils import read_as_of, read_changes, table_properties, validate_table_ref

log = get_logger("delta_tools")
KEYS = {"orders": ["order_id", "updated_at"], "customers": ["customer_id"], "products": ["product_id"],
        "sellers": ["seller_id"], "order_items": ["order_id", "order_item_id"],
        "order_payments": ["order_id", "payment_sequential"]}


def show_history(spark, path, limit):
    from delta.tables import DeltaTable

    rows = (DeltaTable.forPath(spark, path).history(limit)
            .select("version", "timestamp", "operation", "operationMetrics").collect())
    log.info("%-7s %-26s %-18s %s", "version", "timestamp", "operation", "rows (inserted/updated/output)")
    for r in rows:
        m = r["operationMetrics"] or {}
        log.info("%-7s %-26s %-18s ins=%s upd=%s out=%s", r["version"], r["timestamp"], r["operation"],
                 m.get("numTargetRowsInserted", "-"), m.get("numTargetRowsUpdated", "-"),
                 m.get("numOutputRows", "-"))
    props = table_properties(spark, path)
    log.info("properties: %s", {k: v for k, v in props.items() if k.startswith("delta.")})


def show_as_of(spark, path, table, version, timestamp):
    old = read_as_of(spark, path, version, timestamp)
    cur = spark.read.format("delta").load(path)
    keys = [k for k in KEYS.get(table.split("/")[1], []) if k in old.columns and k in cur.columns]
    log.info("as of %s: %d rows | current: %d rows", f"version {version}" if version is not None else timestamp,
             old.count(), cur.count())
    if keys:
        added = cur.select(*keys).subtract(old.select(*keys)).count()
        missing = old.select(*keys).subtract(cur.select(*keys)).count()
        log.info("keys %s: %d added since then, %d missing now (missing = deleted or lost)", keys, added, missing)


def cdf_enabled_since(spark, path):
    """First version from which Change Data Feed data exists (the commit that enabled it), or None."""
    from delta.tables import DeltaTable

    rows = (DeltaTable.forPath(spark, path).history()
            .select("version", F.col("operationParameters").cast("string").alias("params"))
            .orderBy("version").collect())
    for r in rows:
        if "delta.enableChangeDataFeed" in (r["params"] or "") and '"true"' in r["params"].replace("\\", ""):
            return int(r["version"])
    return None


def show_changes(spark, path, start, end):
    since = cdf_enabled_since(spark, path)
    if since is None:
        raise SystemExit(f"Change Data Feed is not enabled on {path}")
    if start < since:
        log.warning("CDF was enabled at version %d; no change data exists before it, starting there", since)
        start = since
    changes = read_changes(spark, path, start, end)
    summary = (changes.groupBy("_commit_version", "_change_type").count()
               .orderBy("_commit_version", "_change_type").collect())
    if not summary:
        log.info("no changes in the requested versions")
    for r in summary:
        log.info("version %s: %-16s %d rows", r["_commit_version"], r["_change_type"], r["count"])
    sample = changes.orderBy(F.desc("_commit_version")).limit(5).drop("ingestion_ts").collect()
    for r in sample:
        log.info("sample: %s", {k: r[k] for k in list(r.asDict())[:4] + ["_change_type", "_commit_version"]})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["history", "as-of", "changes"])
    ap.add_argument("--table", required=True, help="bronze/<table> or silver/<table>")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--version", type=int)
    ap.add_argument("--timestamp")
    ap.add_argument("--from-version", type=int)
    ap.add_argument("--to-version", type=int)
    a = ap.parse_args()
    table = validate_table_ref(a.table)
    path = f"{LAKE}/{table}"
    spark = get_spark("delta_tools")
    try:
        if a.action == "history":
            show_history(spark, path, a.limit)
        elif a.action == "as-of":
            show_as_of(spark, path, table, a.version, a.timestamp)
        else:
            if a.from_version is None:
                raise SystemExit("changes needs --from-version")
            show_changes(spark, path, a.from_version, a.to_version)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
