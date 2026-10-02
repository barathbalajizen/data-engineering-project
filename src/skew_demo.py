"""Data-skew demo: one seller owns most of the rows. Compare four ways to run the same join.

  naive     sort-merge join, AQE off      -> the hot key lands in ONE partition / task
  broadcast broadcast the small side      -> no shuffle of the big side at all
  salted    salt the hot key              -> hot key spread over N partitions
  aqe       Spark 3 adaptive skew join    -> Spark splits the hot partition at runtime

Usage (inside the container):
  python skew_demo.py --rows 3000000 --hot-share 0.6 --hold 300
Open http://localhost:4040 while it runs/holds: Stages -> the join stage -> Summary Metrics
(compare Max vs Median for shuffle read size and duration).
"""
import argparse
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

from common import get_logger, get_spark
from transforms import key_distribution, salted_join

log = get_logger("skew_demo")
PARTS = 16


def build_data(spark, rows, hot_share, n_sellers=200):
    states = ["SP", "RJ", "MG", "PR"]
    sellers = spark.createDataFrame(
        [("sel_HOT", "XX")] + [(f"sel{i:05d}", states[i % 4]) for i in range(n_sellers)],
        ["seller_id", "seller_state"])
    other = F.concat(F.lit("sel"), F.format_string("%05d", (F.rand(7) * n_sellers).cast("int")))
    items = (spark.range(rows).withColumn("r", F.rand(42))
             .withColumn("seller_id", F.when(F.col("r") < hot_share, F.lit("sel_HOT")).otherwise(other))
             .withColumn("price", F.round(F.rand(9) * 200 + 5, 2))
             .drop("r")
             .repartition(PARTS)                       # evenly sized input partitions, like evenly split files
             .persist(StorageLevel.MEMORY_AND_DISK))
    items.count()
    return items, sellers


def partition_stats(df):
    """Rows per output partition of the join: (num_partitions, max, median, total)."""
    counts = sorted(r["count"] for r in df.groupBy(F.spark_partition_id().alias("pid")).count().collect())
    return len(counts), counts[-1], counts[len(counts) // 2], sum(counts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=3_000_000)
    ap.add_argument("--hot-share", type=float, default=0.6)
    ap.add_argument("--salts", type=int, default=PARTS)
    ap.add_argument("--hold", type=int, default=0, help="keep Spark UI alive N seconds at the end")
    args = ap.parse_args()

    spark = get_spark("skew_demo", ui=True)
    items, sellers = build_data(spark, args.rows, args.hot_share)

    log.info("Key distribution (top 3):")
    for k, c, share in key_distribution(items, "seller_id", 3):
        log.info("  %-10s %9d rows  %5.1f%%", k, c, share * 100)

    base = {"spark.sql.shuffle.partitions": str(PARTS), "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.enabled": "false"}
    aqe = {**base, "spark.sql.adaptive.enabled": "true",
           "spark.sql.adaptive.skewJoin.enabled": "true",
           "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
           "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1MB",
           "spark.sql.adaptive.advisoryPartitionSizeInBytes": "4MB",
           "spark.sql.adaptive.coalescePartitions.enabled": "false"}

    variants = [
        ("naive (SMJ)", base, lambda: items.join(sellers, "seller_id"), True),
        ("broadcast", base, lambda: items.join(F.broadcast(sellers), "seller_id"), True),
        (f"salted x{args.salts}", base, lambda: salted_join(items, sellers, "seller_id", args.salts), True),
        ("AQE skew join", aqe, lambda: items.join(sellers, "seller_id"), False),
    ]

    items.limit(1000).join(sellers, "seller_id").count()   # warm-up so the first variant isn't penalised
    results = []
    for name, conf, make, want_stats in variants:
        for k, v in conf.items():
            spark.conf.set(k, v)
        df = make()
        t0 = time.time()
        df.write.format("noop").mode("overwrite").save()    # forces the full join, writes nothing
        secs = time.time() - t0
        if want_stats:
            n, mx, med, total = partition_stats(df)
        else:
            total, n, mx, med = df.count(), None, None, None  # AQE reshapes partitions at runtime
        assert total == args.rows, f"{name}: row count {total} != {args.rows} (join lost/duplicated rows)"
        results.append((name, secs, n, mx, med))
        log.info("finished %s in %.1fs", name, secs)

    print("\n%-16s %8s %11s %12s %12s %11s" % ("variant", "time(s)", "partitions", "max rows", "median rows", "max/median"))
    for name, secs, n, mx, med in results:
        if n is None:
            print("%-16s %8.1f %11s %12s %12s %11s" % (name, secs, "runtime", "see UI", "see UI", "n/a"))
        else:
            print("%-16s %8.1f %11d %12d %12d %10.1fx" % (name, secs, n, mx, med, mx / max(med, 1)))
    print("\nTimings depend on your machine; the partition columns show the skew itself.")
    print("For AQE look for 'SortMergeJoin(skew=true)' in the Spark UI SQL tab.")

    if args.hold:
        log.info("Spark UI: http://localhost:4040 (holding %ds)", args.hold)
        time.sleep(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
