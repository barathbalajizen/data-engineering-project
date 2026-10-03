"""Delta Lake maintenance: compact small files (OPTIMIZE) and report or remove unreferenced files (VACUUM).

Why: every incremental MERGE adds new data files, so a table read daily slowly accumulates many small files
and each read pays a per-file cost (measured in docs/performance.md: 100 files vs 1 file of the same rows).
OPTIMIZE rewrites them into larger files; queries and time travel are unaffected (it is a new version).

  python maintenance.py [--min-files 16] [--small-file-mb 32] [--vacuum]

A table is compacted when it has at least --min-files files AND their average size is below --small-file-mb.
VACUUM is a dry run (lists what it would delete) unless --vacuum is given; it always uses the table's own
retention (delta.deletedFileRetentionDuration, 7 days by default), so time travel within that window keeps
working. Each table is audited in audit.pipeline_run_log; the file counts are in audit.lineage.
"""
import argparse
import os

from audit import audit_step, make_batch_id
from common import LAKE, get_engine, get_logger, get_spark

log = get_logger("maintenance")


def lake_tables():
    """bronze/<t>, silver/<t> and silver/_quarantine/<t> table paths that exist."""
    out = []
    for layer in ("bronze", "silver", "silver/_quarantine"):
        root = os.path.join(LAKE, layer)
        if os.path.isdir(root):
            out += [f"{layer}/{t}" for t in sorted(os.listdir(root))
                    if not t.startswith("_") and os.path.isdir(os.path.join(root, t, "_delta_log"))]
    return out


def needs_compaction(num_files, size_bytes, min_files, small_file_mb):
    """Pure decision (unit-tested): many files that are small on average."""
    if num_files < max(min_files, 2):
        return False
    return size_bytes / num_files < small_file_mb * 1024 * 1024


def maintain(spark, eng, ref, min_files, small_file_mb, vacuum):
    from delta.tables import DeltaTable

    path = os.path.join(LAKE, ref)
    with audit_step(table_name=ref.replace("/", "."), batch_id=make_batch_id(ref.split("/")[-1]),
                    engine=eng) as a:
        table = DeltaTable.forPath(spark, path)
        files, size = table.detail().select("numFiles", "sizeInBytes").first()
        a.source_rows, a.inserted, a.updated, a.rejected = files, 0, 0, 0
        detail = f"files={files} size_mb={size / 1e6:.2f}"
        if needs_compaction(files, size, min_files, small_file_mb):
            table.optimize().executeCompaction()
            after = table.detail().select("numFiles").first()[0]
            detail += f" -> OPTIMIZE -> files={after}"
            log.info("%s: compacted %d -> %d files", ref, files, after)
        else:
            log.info("%s: %d file(s), %.2f MB: no compaction needed", ref, files, size / 1e6)
        if vacuum:
            spark.sql(f"VACUUM delta.`{path}`")   # table retention, never shorter
            detail += " VACUUM done"
        else:
            removable = spark.sql(f"VACUUM delta.`{path}` DRY RUN").count()
            detail += f" vacuum_dry_run_files={removable}"
            if removable:
                log.info("%s: VACUUM would delete %d unreferenced file(s) older than the retention", ref, removable)
        a.lineage(f"delta:{ref}", f"delta:{ref}", None, None, detail)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--min-files", type=int, default=16)
    ap.add_argument("--small-file-mb", type=float, default=32)
    ap.add_argument("--vacuum", action="store_true", help="really delete unreferenced files (default: dry run)")
    a = ap.parse_args()
    spark, eng = get_spark("maintenance"), get_engine()
    # VACUUM lists files with a Spark job of `parallelPartitionDiscovery.parallelism` tasks (default 10000):
    # measured 128-164 s for a dry run on a 3 KB table vs 11-13 s with 8 tasks (docs/performance.md)
    spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism",
                   os.getenv("VACUUM_LISTING_PARALLELISM", "8"))
    try:
        for ref in lake_tables():
            maintain(spark, eng, ref, a.min_files, a.small_file_mb, a.vacuum)
    finally:
        spark.stop()
        eng.dispose()


if __name__ == "__main__":
    main()
