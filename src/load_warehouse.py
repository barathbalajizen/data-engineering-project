"""Silver (Delta) -> Postgres `staging` schema (input for dbt).
Truncate and reload, so reruns are safe. Each table is audited; the inserted count is the row count read back
from Postgres after the load, so the audit row verifies the write instead of trusting the writer.
"""
import sqlalchemy as sa

from audit import audit_step, delta_last_operation, make_batch_id
from common import get_engine, get_logger, get_spark, jdbc_write_overwrite, silver_path

log = get_logger("load_warehouse")
TABLES = ["orders", "customers", "products", "sellers", "order_items", "order_payments"]


def load_table(spark, eng, t):
    with audit_step(table_name=f"staging.{t}", batch_id=make_batch_id(t), engine=eng) as a:
        version, _ = delta_last_operation(spark, silver_path(t))
        df = spark.read.format("delta").option("versionAsOf", version).load(silver_path(t))
        a.source_rows = df.count()
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
