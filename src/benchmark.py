"""Performance benchmarks on COPIES of the data (nothing real is modified).

Spark/Delta work goes to <LAKE>/_bench (LAKE_PATH must point there; the real lake is read from
REAL_LAKE_PATH), Postgres work to the `bench` schema; both are removed at the end. Every case runs the real
pipeline functions, repeated `--repeat` times, at each `--scales` factor (1 = today's volume; N = N copies
of every order with new keys). Results go to audit.benchmark_results.

  LAKE_PATH=/app/lake/_bench python benchmark.py --label baseline [--scales 1 10] [--repeat 3] [--only silver]

Cases
  silver_orders   incremental (Change Data Feed) vs full pass vs skip, after a batch of 1% updated + 0.5% new orders
  extract_read    JDBC read of source orders: full vs incremental window; fetchsize default vs 10000
  jdbc_write      Silver -> Postgres: batchsize 1000 (Spark default) / 10000 / 50000, 1 / 2 / 4 partitions
  shuffle         validate + dedupe + write of orders with spark.sql.shuffle.partitions 2 / 8 / 200 (files written)
  small_files     read + aggregate a table written in 100 small appends, before and after OPTIMIZE
"""
import argparse
import json
import os
import shutil
import statistics
import time
import uuid

import sqlalchemy as sa
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import JDBC_URL, LAKE, PG, get_engine, get_logger, get_spark

log = get_logger("benchmark")
REAL_LAKE = os.getenv("REAL_LAKE_PATH", "/app/lake")
BENCH_SCHEMA = "bench"


class Step:
    """Stands in for audit_step inside benchmarks (no audit rows for synthetic work)."""
    source_rows = inserted = updated = rejected = None

    def lineage(self, *a, **k):
        pass

    def schema_changes(self, *a, **k):
        pass

    def rejections(self, *a, **k):
        pass


class Recorder:
    def __init__(self, eng, label):
        self.eng, self.suite = eng, f"{label}-{time.strftime('%Y%m%dT%H%M%S')}"
        self.env = {"cpus": os.cpu_count(), "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"), "label": label}

    def record(self, benchmark, variant, scale, rows, seconds, **details):
        s = sorted(seconds)
        row = {"suite": self.suite, "b": benchmark, "v": variant, "scale": scale, "rows": rows, "runs": len(s),
               "min": s[0], "med": statistics.median(s), "max": s[-1],
               "details": json.dumps({**self.env, **details}, default=str)}
        with self.eng.begin() as c:
            c.execute(sa.text("INSERT INTO audit.benchmark_results (suite_run, benchmark, variant, scale, "
                              "rows_processed, runs, min_seconds, median_seconds, max_seconds, details) VALUES "
                              "(:suite, :b, :v, :scale, :rows, :runs, :min, :med, :max, CAST(:details AS jsonb))"),
                      row)
        log.info("RESULT %-14s %-28s x%-3d rows=%-8s median=%7.2fs min=%7.2fs max=%7.2fs %s", benchmark, variant,
                 scale, rows, row["med"], row["min"], row["max"], details or "")


def timed(fn, repeat, before=None, warmup=0):
    """Run fn `repeat` times (calling `before` untimed first each time). Returns (seconds list, last result).
    warmup: untimed runs first, so the first variant of a case does not also pay JVM/JDBC/cache warm-up."""
    for _ in range(warmup):
        if before:
            before()
        fn()
    out, result = [], None
    for _ in range(repeat):
        if before:
            before()
        t0 = time.perf_counter()
        result = fn()
        out.append(time.perf_counter() - t0)
    return out, result


def num_files(spark, path):
    return DeltaTable.forPath(spark, path).detail().select("numFiles").first()[0]


def scaled(df, scale, key="order_id"):
    """`scale` copies of df with distinct keys (copy 0 keeps the original keys)."""
    copies = [df] + [df.withColumn(key, F.concat(F.col(key), F.lit(f"_x{i}"))) for i in range(1, scale)]
    out = copies[0]
    for c in copies[1:]:
        out = out.unionByName(c)
    return out


def bench_path(*parts):
    return os.path.join(LAKE, *parts)


# ---------------------------------------------------------------- silver_orders
def bench_silver(spark, rec, scale, repeat):
    from common import bronze_path, silver_path
    from extract_bronze import write_bronze
    from incremental import MemoryCheckpointStore
    from transform_silver import SPECS, process_incremental

    bpath, spath = bronze_path("orders"), silver_path("orders")
    cfg = {"mode": "incremental", "version_keys": ["order_id", "updated_at"], "cdf": True}
    base = scaled(spark.read.format("delta").load(f"{REAL_LAKE}/bronze/orders"), scale).cache()
    n = base.count()
    write_bronze(spark, base, bpath, cfg, None)                       # bench Bronze with CDF on
    store = MemoryCheckpointStore()
    process_incremental(spark, Step(), "orders", SPECS["orders"], store, True)   # initial Silver
    silver_v0 = DeltaTable.forPath(spark, spath).history(1).first()["version"]
    bronze_v0 = store.get("orders")

    # A day of changes: 1% of orders get a newer version, 0.5% new orders
    upd = (base.sample(fraction=0.01, seed=1)
           .withColumn("updated_at", F.col("updated_at") + F.expr("INTERVAL 1 DAY"))
           .withColumn("order_status", F.lit("delivered")))
    new = (base.sample(fraction=0.005, seed=2).withColumn("order_id", F.concat("order_id", F.lit("_new"))))
    changes = upd.unionByName(new).cache()
    n_changes = changes.count()
    from delta_utils import delta_schema
    write_bronze(spark, changes, bpath, cfg, delta_schema(spark, bpath))
    bronze_v1 = DeltaTable.forPath(spark, bpath).history(1).first()["version"]

    def reset(checkpoint):
        def _r():
            DeltaTable.forPath(spark, spath).restoreToVersion(silver_v0)
            store.set("orders", checkpoint, "bench")
        return _r

    def run(force):
        a = Step()
        process_incremental(spark, a, "orders", SPECS["orders"], store, force)
        return a

    secs, a = timed(lambda: run(False), repeat, reset(bronze_v0))
    rec.record("silver_orders", "incremental", scale, a.source_rows, secs, changes=n_changes, table_rows=n,
               inserted=a.inserted, updated=a.updated)
    secs, a = timed(lambda: run(True), repeat, reset(bronze_v0))
    rec.record("silver_orders", "full", scale, a.source_rows, secs, changes=n_changes, table_rows=n,
               inserted=a.inserted, updated=a.updated)
    secs, a = timed(lambda: run(False), repeat, lambda: store.set("orders", bronze_v1, "bench"))
    rec.record("silver_orders", "skip (no new data)", scale, a.source_rows, secs, table_rows=n)
    base.unpersist()
    changes.unpersist()


# ---------------------------------------------------------------- shuffle / files
def bench_shuffle(spark, rec, scale, repeat):
    from transform_silver import SPECS, validate

    base = scaled(spark.read.format("delta").load(f"{REAL_LAKE}/bronze/orders"), scale).cache()
    n = base.count()
    previous = spark.conf.get("spark.sql.shuffle.partitions")
    for parts in (2, 8, 200):
        out = bench_path(f"shuffle_{parts}")
        spark.conf.set("spark.sql.shuffle.partitions", str(parts))

        def work():
            latest, invalid, _, _ = validate(base, SPECS["orders"])
            latest.write.format("delta").mode("overwrite").save(out)
            latest.unpersist()
            invalid.unpersist()

        secs, _ = timed(work, repeat, warmup=1)
        rec.record("shuffle", f"shuffle.partitions={parts}", scale, n, secs, files_written=num_files(spark, out),
                   aqe=spark.conf.get("spark.sql.adaptive.enabled"))
    spark.conf.set("spark.sql.shuffle.partitions", previous)
    base.unpersist()


def bench_small_files(spark, rec, scale, repeat, appends=100):
    path = bench_path("small_files")
    base = scaled(spark.read.format("delta").load(f"{REAL_LAKE}/bronze/orders"), scale)
    chunks = base.withColumn("_chunk", F.abs(F.hash("order_id")) % appends).cache()
    n = chunks.count()
    for i in range(appends):   # what many small incremental loads leave behind
        chunks.filter(F.col("_chunk") == i).drop("_chunk").coalesce(1).write.format("delta").mode("append").save(path)
    chunks.unpersist()

    def query():
        return (spark.read.format("delta").load(path).groupBy("order_status")
                .agg(F.count("*"), F.max("updated_at")).collect())

    before = num_files(spark, path)
    secs, _ = timed(query, repeat)
    rec.record("small_files", "before OPTIMIZE", scale, n, secs, files=before)
    t0 = time.perf_counter()
    DeltaTable.forPath(spark, path).optimize().executeCompaction()
    optimize_s = time.perf_counter() - t0
    after = num_files(spark, path)
    secs, _ = timed(query, repeat)
    rec.record("small_files", "after OPTIMIZE", scale, n, secs, files=after, optimize_seconds=round(optimize_s, 2))


# ---------------------------------------------------------------- JDBC
def jdbc_reader(spark, dbtable, fetchsize=None):
    r = (spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", dbtable)
         .option("user", PG["user"]).option("password", PG["password"]).option("driver", "org.postgresql.Driver"))
    return (r.option("fetchsize", str(fetchsize)) if fetchsize else r).load()


def bench_extract_read(spark, eng, rec, scale, repeat):
    src = f"{BENCH_SCHEMA}.orders_src_x{scale}"
    with eng.begin() as c:   # scaled copy of source.orders in the bench schema
        c.execute(sa.text(f"DROP TABLE IF EXISTS {src}"))
        c.execute(sa.text(f"CREATE TABLE {src} AS SELECT * FROM source.orders WHERE false"))
        for i in range(scale):
            suffix = "" if i == 0 else f"_x{i}"
            c.execute(sa.text(f"INSERT INTO {src} SELECT order_id || '{suffix}', customer_id, order_status, "
                              "order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, "
                              "updated_at FROM source.orders"))
        c.execute(sa.text(f"CREATE INDEX ON {src} (updated_at)"))
        c.execute(sa.text(f"ANALYZE {src}"))
        max_ts = c.execute(sa.text(f"SELECT max(updated_at) FROM {src}")).scalar()
    out = bench_path("extract_read")
    window = f"(SELECT * FROM {src} WHERE updated_at >= TIMESTAMP '{max_ts}' - INTERVAL '10 minutes') q"
    for variant, dbtable, fetch in (("full, fetchsize default", src, None), ("full, fetchsize=10000", src, 10000),
                                    ("incremental window (10 min)", window, None)):
        def work():
            df = jdbc_reader(spark, dbtable, fetch)
            df.write.format("delta").mode("overwrite").save(out)
            return spark.read.format("delta").load(out).count()

        secs, rows = timed(work, repeat, warmup=1)
        rec.record("extract_read", variant, scale, rows, secs)


def bench_jdbc_write(spark, eng, rec, scale, repeat):
    df = scaled(spark.read.format("delta").load(f"{REAL_LAKE}/silver/orders"), scale).cache()
    n = df.count()
    target = f"{BENCH_SCHEMA}.orders_w"
    with eng.begin() as c:
        c.execute(sa.text(f"DROP TABLE IF EXISTS {target}"))
    for parts in (1, 2, 4):
        for batch in (1000, 10000, 50000):
            def work():
                (df.repartition(parts).write.format("jdbc").option("url", JDBC_URL).option("dbtable", target)
                 .option("user", PG["user"]).option("password", PG["password"])
                 .option("driver", "org.postgresql.Driver").option("truncate", "true")
                 .option("batchsize", str(batch)).mode("overwrite").save())

            secs, _ = timed(work, repeat, warmup=1)
            rec.record("jdbc_write", f"partitions={parts} batchsize={batch}", scale, n, secs)
    df.unpersist()


CASES = ("silver", "extract_read", "jdbc_write", "shuffle", "small_files")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True, help="e.g. baseline / after-tuning")
    ap.add_argument("--scales", type=int, nargs="+", default=[1, 10])
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--only", nargs="+", choices=CASES, default=list(CASES))
    a = ap.parse_args()
    if os.path.abspath(LAKE) == os.path.abspath(REAL_LAKE) or "_bench" not in LAKE:
        raise SystemExit(f"LAKE_PATH must point to a _bench folder, not the real lake (got {LAKE})")

    eng = get_engine()
    with eng.begin() as c:
        c.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {BENCH_SCHEMA}"))
    rec, spark = Recorder(eng, a.label), get_spark("benchmark")
    log.info("suite %s, scales %s, repeat %d, cases %s", rec.suite, a.scales, a.repeat, a.only)
    try:
        for scale in a.scales:
            shutil.rmtree(LAKE, ignore_errors=True)
            for case in a.only:
                t0 = time.perf_counter()
                {"silver": lambda: bench_silver(spark, rec, scale, a.repeat),
                 "extract_read": lambda: bench_extract_read(spark, eng, rec, scale, a.repeat),
                 "jdbc_write": lambda: bench_jdbc_write(spark, eng, rec, scale, a.repeat),
                 "shuffle": lambda: bench_shuffle(spark, rec, scale, a.repeat),
                 "small_files": lambda: bench_small_files(spark, rec, scale, a.repeat)}[case]()
                log.info("case %s x%d done in %.0fs", case, scale, time.perf_counter() - t0)
                shutil.rmtree(LAKE, ignore_errors=True)
    finally:
        spark.stop()
        shutil.rmtree(LAKE, ignore_errors=True)
        with eng.begin() as c:
            c.execute(sa.text(f"DROP SCHEMA IF EXISTS {BENCH_SCHEMA} CASCADE"))
        eng.dispose()
    log.info("suite %s finished (results in audit.benchmark_results)", rec.suite)


if __name__ == "__main__":
    os.environ.setdefault("RUN_ID", f"benchmark-{uuid.uuid4().hex[:8]}")
    main()
