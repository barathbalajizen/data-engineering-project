"""Delta Lake helpers: schema, controlled evolution, table properties, time travel and Change Data Feed."""
import contextlib
import re

from pyspark.sql import functions as F

from schema_drift import schema_dict

AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"
TABLE_REF = re.compile(r"^(bronze|silver)/[a-z_]+$")


def is_delta(spark, path):
    from delta.tables import DeltaTable
    return DeltaTable.isDeltaTable(spark, path)


def delta_schema(spark, path):
    """{column: type} of a Delta table, or None if it does not exist yet."""
    if not is_delta(spark, path):
        return None
    return schema_dict(spark.read.format("delta").load(path).schema)


def align_to_target(df, report, target_types, allow_removed=False):
    """Apply an accepted DriftReport: cast narrower columns to the target type and, when allowed, add
    removed columns back as typed NULLs. Added columns are left for Delta schema evolution."""
    for col, _, target_type in report.casts:
        df = df.withColumn(col, F.col(col).cast(target_type))
    if allow_removed:
        for col, _ in report.removed:
            df = df.withColumn(col, F.lit(None).cast(target_types[col]))
    return df


@contextlib.contextmanager
def schema_evolution(spark, enabled=True):
    """Enable Delta MERGE schema evolution only for the enclosed write (only after the drift policy has
    accepted the change), then restore the previous setting."""
    previous = spark.conf.get(AUTO_MERGE, "false")
    spark.conf.set(AUTO_MERGE, "true" if enabled else "false")
    try:
        yield
    finally:
        spark.conf.set(AUTO_MERGE, previous)


def table_properties(spark, path):
    return dict(spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("properties").first()[0] or {})


def ensure_table_properties(spark, path, props):
    """Set TBLPROPERTIES that differ from the wanted values. Returns the keys changed. Only commits (creates
    a new table version) when something actually changes, so it is cheap to call on every run."""
    current = table_properties(spark, path)
    todo = {k: v for k, v in props.items() if str(current.get(k, "")).lower() != str(v).lower()}
    if todo:
        assignments = ", ".join(f"'{k}' = '{v}'" for k, v in todo.items())
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({assignments})")
    return sorted(todo)


# ------------------------------------------------------------ time travel / CDF
def validate_table_ref(ref):
    """'bronze/orders' style reference (no path traversal)."""
    if not TABLE_REF.match(ref or ""):
        raise ValueError(f"table must look like bronze/orders or silver/customers, got {ref!r}")
    return ref


def read_as_of(spark, path, version=None, timestamp=None):
    """Time travel: the table as it was at a version or timestamp (one of them)."""
    if (version is None) == (timestamp is None):
        raise ValueError("give exactly one of version or timestamp")
    reader = spark.read.format("delta")
    reader = reader.option("versionAsOf", int(version)) if version is not None else \
        reader.option("timestampAsOf", str(timestamp))
    return reader.load(path)


def read_changes(spark, path, start_version, end_version=None):
    """Change Data Feed rows (with _change_type, _commit_version, _commit_timestamp) between versions.
    The table must have delta.enableChangeDataFeed=true at start_version."""
    reader = (spark.read.format("delta").option("readChangeFeed", "true")
              .option("startingVersion", int(start_version)))
    if end_version is not None:
        reader = reader.option("endingVersion", int(end_version))
    return reader.load(path)
