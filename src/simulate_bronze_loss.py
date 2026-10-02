"""Delete a time window from Bronze orders to simulate data loss. Restore it with a backfill:
   run_pipeline.sh --start <same start> --end <same end>
"""
import argparse
from datetime import datetime

from delta.tables import DeltaTable

from common import bronze_path, get_logger, get_spark

log = get_logger("simulate_loss")
ap = argparse.ArgumentParser()
ap.add_argument("--start", required=True)
ap.add_argument("--end", required=True)
a = ap.parse_args()
lo = datetime.fromisoformat(a.start).isoformat(sep=" ")
hi = datetime.fromisoformat(a.end).isoformat(sep=" ")

spark = get_spark("simulate_loss")
t = DeltaTable.forPath(spark, bronze_path("orders"))
before = t.toDF().count()
t.delete(f"updated_at >= TIMESTAMP '{lo}' AND updated_at < TIMESTAMP '{hi}'")
log.info("bronze.orders rows %d -> %d (deleted window [%s, %s))", before, t.toDF().count(), lo, hi)
spark.stop()
