"""Shared config, logging, DB and Spark helpers."""
import json
import logging
import os

import sqlalchemy as sa

PG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": os.getenv("PG_PORT", "5432"),
    "db": os.getenv("PG_DB", "shop"),
    "user": os.getenv("PG_USER", "de"),
    "password": os.getenv("PG_PASSWORD", "de"),
}
LAKE = os.getenv("LAKE_PATH", "/app/lake")
DATA = os.getenv("DATA_PATH", "/app/data/raw")
RUN_ID = os.getenv("RUN_ID", "manual")
TASK_NAME = os.getenv("TASK_NAME", "-")
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['db']}"
JDBC_JAR = os.getenv("PG_JDBC_JAR", "/opt/jars/postgresql.jar")

# Performance settings (measured in docs/performance.md)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
# The Postgres JDBC driver buffers the WHOLE result set unless a fetch size is set (and autocommit is off,
# which Spark does for reads): rows are then streamed in batches of this size.
JDBC_FETCH_SIZE = int(os.getenv("JDBC_FETCH_SIZE", "10000"))
JDBC_BATCH_SIZE = int(os.getenv("JDBC_BATCH_SIZE", "10000"))   # rows per INSERT batch (Spark default: 1000)
JDBC_WRITE_PARTITIONS = int(os.getenv("JDBC_WRITE_PARTITIONS", "4"))   # parallel connections for large writes
JDBC_ROWS_PER_WRITER = 50000   # below this a table is written over one connection (no extra shuffle)
ROWS_PER_FILE = int(os.getenv("DELTA_ROWS_PER_FILE", "1000000"))   # target rows per Delta data file


class JsonFormatter(logging.Formatter):
    """One JSON object per line (LOG_FORMAT=json), with run and task ids for log search tools."""

    def format(self, record):
        out = {"ts": self.formatTime(record), "level": record.levelname, "run_id": RUN_ID, "task": TASK_NAME,
               "logger": record.name, "msg": record.getMessage()}
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        if os.getenv("LOG_FORMAT", "text").lower() == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                f"%(asctime)s | %(levelname)s | {RUN_ID} | {TASK_NAME} | %(name)s | %(message)s"))
        logging.basicConfig(level=logging.INFO, handlers=[handler])
    return logging.getLogger(name)


def get_engine():
    url = (
        f"postgresql+psycopg2://{PG['user']}:{PG['password']}"
        f"@{PG['host']}:{PG['port']}/{PG['db']}"
    )
    # connect_timeout: fail fast (and let the task retry) instead of hanging when Postgres is down
    return sa.create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def get_spark(app_name: str, ui: bool = False):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", JDBC_JAR)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.enabled", "true" if ui else "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_read(spark, dbtable: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", dbtable)
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", str(JDBC_FETCH_SIZE))
        .load()
    )


def jdbc_write_partitions(rows: int) -> int:
    """Parallel JDBC writers for a table of `rows` rows: one per JDBC_ROWS_PER_WRITER, at most
    JDBC_WRITE_PARTITIONS (measured: 200k rows 10.6s on 1 connection with batch 1000, 4.5s on 4 with 10000)."""
    return max(1, min(JDBC_WRITE_PARTITIONS, int(rows) // JDBC_ROWS_PER_WRITER))


def jdbc_write_overwrite(df, dbtable: str, rows: int | None = None):
    # truncate=true keeps the table definition (and its indexes) and just empties it
    if rows is not None:
        df = df.repartition(jdbc_write_partitions(rows))
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", dbtable)
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .option("batchsize", str(JDBC_BATCH_SIZE))
        .mode("overwrite")
        .save()
    )


def sized_for_write(df, rows: int):
    """Coalesce to about ROWS_PER_FILE rows per output file (at least one). Without it a write after a
    shuffle produces one file per shuffle partition, e.g. 8 files for a 200-row table; AQE cannot merge them
    when the DataFrame is cached."""
    return df.coalesce(max(1, -(-int(rows) // ROWS_PER_FILE)))


def bronze_path(table: str) -> str:
    return f"{LAKE}/bronze/{table}"


def silver_path(table: str) -> str:
    return f"{LAKE}/silver/{table}"


def quarantine_path(table: str) -> str:
    return f"{LAKE}/silver/_quarantine/{table}"
