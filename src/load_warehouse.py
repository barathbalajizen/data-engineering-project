"""Silver (Delta) -> Postgres `staging` schema (input for dbt)."""
from common import get_logger, get_spark, jdbc_write_overwrite, silver_path

log = get_logger("load_warehouse")
TABLES = ["orders", "customers", "products", "sellers", "order_items", "order_payments"]


def main():
    spark = get_spark("load_warehouse")
    for t in TABLES:
        df = spark.read.format("delta").load(silver_path(t))
        jdbc_write_overwrite(df, f"staging.{t}")
        log.info("staging.%s loaded", t)
    spark.stop()


if __name__ == "__main__":
    main()
