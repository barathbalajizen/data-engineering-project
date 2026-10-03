"""Silver (Delta) -> Postgres `staging` schema (input for dbt).
Truncate and reload, so reruns are safe. Each table is audited; the inserted count is the row count read back
from Postgres after the load, so the audit row verifies the write instead of trusting the writer.
Columns accepted upstream (Bronze schema drift policy) but missing in the staging table are added with
ALTER TABLE ... ADD COLUMN and recorded in audit.schema_changes; the truncate-mode JDBC write keeps the
table definition, so it would otherwise fail on the unknown column.
"""
import sqlalchemy as sa

from audit import audit_step, delta_last_operation, make_batch_id
from common import get_engine, get_logger, get_spark, jdbc_write_overwrite, silver_path
from schema_drift import schema_dict

log = get_logger("load_warehouse")
TABLES = ["orders", "customers", "products", "sellers", "order_items", "order_payments"]
SCHEMA = "staging"

# Spark simpleString type -> Postgres type (what the Spark JDBC writer itself uses for new tables)
SPARK_TO_PG = {"string": "TEXT", "int": "INTEGER", "bigint": "BIGINT", "smallint": "SMALLINT",
               "tinyint": "SMALLINT", "double": "DOUBLE PRECISION", "float": "REAL", "boolean": "BOOLEAN",
               "timestamp": "TIMESTAMP", "date": "DATE"}


def pg_type(spark_type):
    if spark_type.startswith("decimal"):
        return spark_type.replace("decimal", "NUMERIC").upper()
    if spark_type not in SPARK_TO_PG:
        raise ValueError(f"no Postgres type mapping for Spark type {spark_type}")
    return SPARK_TO_PG[spark_type]


def staging_columns(eng, t):
    """{column: data_type} of staging.<t>, or None if the table does not exist yet."""
    with eng.connect() as c:
        rows = c.execute(sa.text("SELECT column_name, data_type FROM information_schema.columns "
                                 "WHERE table_schema = :s AND table_name = :t"), {"s": SCHEMA, "t": t}).fetchall()
    return {r[0]: r[1] for r in rows} or None


def add_missing_columns(eng, a, t, df_schema):
    """Add columns the DataFrame has and the staging table lacks. Returns the columns added."""
    existing = staging_columns(eng, t)
    if existing is None:
        return []   # first load: the JDBC writer creates the table
    added = [(col, typ) for col, typ in schema_dict(df_schema).items() if col not in existing]
    if not added:
        return []
    with eng.begin() as c:
        for col, typ in added:
            c.execute(sa.text(f'ALTER TABLE {SCHEMA}."{t}" ADD COLUMN IF NOT EXISTS "{col}" {pg_type(typ)}'))
    a.schema_changes(f"{SCHEMA}.{t}", [("added", col, None, typ, "evolved") for col, typ in added])
    log.info("%s.%s: added column(s) %s", SCHEMA, t, [c for c, _ in added])
    return [c for c, _ in added]


def load_table(spark, eng, t):
    with audit_step(table_name=f"staging.{t}", batch_id=make_batch_id(t), engine=eng) as a:
        version, _ = delta_last_operation(spark, silver_path(t))
        df = spark.read.format("delta").option("versionAsOf", version).load(silver_path(t))
        a.source_rows = df.count()
        add_missing_columns(eng, a, t, df.schema)
        jdbc_write_overwrite(df, f"staging.{t}")
        with eng.connect() as c:
            a.inserted = c.execute(sa.text(f"SELECT count(*) FROM staging.{t}")).scalar()
        a.updated, a.rejected = 0, 0
        if a.inserted != a.source_rows:
            raise RuntimeError(f"staging.{t}: wrote {a.source_rows} rows but the table has {a.inserted}")
        log.info("staging.%s loaded: %d rows from silver version %d", t, a.inserted, version)
        a.lineage(f"delta:silver/{t}@v{version}", f"postgres:staging.{t}", a.inserted, None, "truncate+reload")


def main():
    spark, eng = get_spark("load_warehouse"), get_engine()
    try:
        for t in TABLES:
            load_table(spark, eng, t)
    finally:
        spark.stop()
        eng.dispose()


if __name__ == "__main__":
    main()
