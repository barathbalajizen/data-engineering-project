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
