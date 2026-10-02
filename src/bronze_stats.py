"""Print Bronze/Silver/quarantine health: proves idempotency (no duplicate order versions)."""
from delta.tables import DeltaTable

from common import bronze_path, get_logger, get_spark, quarantine_path, silver_path

log = get_logger("bronze_stats")
spark = get_spark("bronze_stats")

b = spark.read.format("delta").load(bronze_path("orders"))
rows = b.count()
versions = b.select("order_id", "updated_at").distinct().count()
orders = b.select("order_id").distinct().count()
log.info("bronze.orders rows=%d | distinct orders=%d | distinct versions=%d | DUPLICATE versions=%d",
         rows, orders, versions, rows - versions)
log.info("silver.orders rows=%d", spark.read.format("delta").load(silver_path("orders")).count())
q = quarantine_path("orders")
if DeltaTable.isDeltaTable(spark, q):
    log.info("quarantine.orders rows=%d", spark.read.format("delta").load(q).count())
spark.stop()
