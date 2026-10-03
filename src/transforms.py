"""Pure, unit-testable transformation helpers (no Delta / Postgres needed)."""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_strings(df):
    """Trim whitespace on every string column."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def dedupe_latest(df, keys, order_cols):
    """Keep one row per key: the one with the greatest order_cols (latest wins)."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_valid_invalid(df, keys, extra_cond=None):
    """Return (valid_df, invalid_df). Null keys or a failed/NULL extra condition => invalid."""
    cond = F.lit(True)
    for k in keys:
        cond = cond & F.col(k).isNotNull()
    if extra_cond:
        cond = cond & F.expr(extra_cond)
    cond = F.coalesce(cond, F.lit(False))
    return df.filter(cond), df.filter(~cond)


def key_distribution(df, key, top=5):
    """Top-N keys by row count: [(key, count, share)]. First thing to check when a Spark stage has one slow task."""
    total = df.count()
    rows = df.groupBy(key).count().orderBy(F.desc("count")).limit(top).collect()
    return [(r[key], r["count"], r["count"] / total) for r in rows]


def salted_join(big, small, key, num_salts, how="inner", seed=42):
    """Skew fix: spread a hot key over `num_salts` sub-keys.
    big side   : random salt 0..num_salts-1 per row
    small side : replicated once per salt value (small side must be small!)
    Result equals big.join(small, key, how).
    """
    salted_big = big.withColumn("_salt", (F.rand(seed) * num_salts).cast("int"))
    salted_small = small.withColumn("_salt", F.explode(F.array(*[F.lit(i) for i in range(num_salts)])))
    return salted_big.join(salted_small, [key, "_salt"], how).drop("_salt")


def classify_versions(batch, target, keys, version_col):
    """Compare a batch with the target table, per key. Returns counts of rows that are
    new (key not in target), newer (will update), same (version already there) and late
    (older than the target's version: a late-arriving record that must NOT overwrite newer data).
    target=None means the target does not exist yet (everything is new)."""
    if target is None:
        return {"new": batch.count(), "newer": 0, "same": 0, "late": 0}
    tv = "_target_version"
    joined = batch.select(*keys, version_col).join(target.select(*keys, F.col(version_col).alias(tv)), keys, "left")
    v, t = F.col(version_col), F.col(tv)
    row = joined.agg(
        F.sum(F.when(t.isNull(), 1).otherwise(0)).alias("new"),
        F.sum(F.when(v > t, 1).otherwise(0)).alias("newer"),
        F.sum(F.when(v == t, 1).otherwise(0)).alias("same"),
        F.sum(F.when(v < t, 1).otherwise(0)).alias("late"),
    ).first()
    return {k: int(row[k] or 0) for k in ("new", "newer", "same", "late")}


def reconcile_versions(source, silver, quarantine, key, version_col):
    """Key- and version-level source -> Silver reconciliation (for a table with one key column).

    missing            source keys found neither in Silver nor in quarantine (data was lost: critical)
    quarantined_only   source keys only in quarantine (every version was rejected)
    stale_explained    Silver holds an older version because the newer source version was rejected
    stale_unexplained  Silver is behind the source for no recorded reason (late load or a change made
                       after the extract); ahead = Silver newer than the source (should never happen)
    """
    src = source.select(key, F.col(version_col).alias("_src_v")).dropDuplicates([key])
    sil = silver.select(key, F.col(version_col).alias("_sil_v"))
    j = src.join(sil, key, "left")
    if quarantine is not None:
        q_keys = quarantine.select(F.col(key).alias("_q_key")).distinct()
        q_versions = quarantine.select(F.col(key).alias("_qv_key"), F.col(version_col).alias("_qv")).distinct()
        j = (j.join(q_keys, F.col(key) == F.col("_q_key"), "left")
              .join(q_versions, (F.col(key) == F.col("_qv_key")) & (F.col("_src_v") == F.col("_qv")), "left"))
    else:
        j = j.withColumn("_q_key", F.lit(None)).withColumn("_qv_key", F.lit(None))
    in_sil, in_q = F.col("_sil_v").isNotNull(), F.col("_q_key").isNotNull()
    stale, explained = F.col("_sil_v") < F.col("_src_v"), F.col("_qv_key").isNotNull()
    row = j.agg(
        F.count("*").alias("source"),
        F.sum(F.when(in_sil, 1).otherwise(0)).alias("in_silver"),
        F.sum(F.when(~in_sil & in_q, 1).otherwise(0)).alias("quarantined_only"),
        F.sum(F.when(~in_sil & ~in_q, 1).otherwise(0)).alias("missing"),
        F.sum(F.when(stale & explained, 1).otherwise(0)).alias("stale_explained"),
        F.sum(F.when(stale & ~explained, 1).otherwise(0)).alias("stale_unexplained"),
        F.sum(F.when(F.col("_sil_v") > F.col("_src_v"), 1).otherwise(0)).alias("ahead"),
    ).first()
    return {k: int(row[k] or 0) for k in row.asDict()}
