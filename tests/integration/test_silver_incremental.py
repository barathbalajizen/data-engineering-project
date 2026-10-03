"""Integration test of incremental Silver on Delta Lake: Change Data Feed reads, skip when nothing changed,
late-arriving versions, invalid newer versions, idempotent quarantine log, forced full refresh and Bronze deletes.

Runs the real extract_bronze / transform_silver functions against a temp lake in a subprocess (own Delta
Spark session). Needs delta-spark:  docker compose exec pipeline pytest tests -m integration -v
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("delta")
pytestmark = pytest.mark.integration

SRC = Path(__file__).resolve().parents[2] / "src"

SCRIPT = textwrap.dedent('''
    import json
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from common import bronze_path, get_spark, quarantine_path, silver_path
    from delta_utils import delta_schema
    from extract_bronze import write_bronze
    from incremental import MemoryCheckpointStore
    from transform_silver import SPECS, process_incremental

    class Step:                          # stands in for audit_step
        def __init__(self):
            self.source_rows = self.inserted = self.updated = self.rejected = None
            self.details = []
        def lineage(self, *a, **k): self.details.append(a[4] if len(a) > 4 else k.get("detail"))
        def rejections(self, table, counts, samples=None): self.rejected_report = (table, counts, samples)
        def schema_changes(self, *a): pass

    spark = get_spark("test_silver")
    cfg = {"mode": "incremental", "version_keys": ["order_id", "updated_at"], "cdf": True}
    bpath, spath = bronze_path("orders"), silver_path("orders")
    store, out = MemoryCheckpointStore(), {}

    def orders(rows):
        # (order_id, status, purchase day, delivered day or None, updated day)
        sql = " union all ".join(
            f"select '{o}' order_id, 'c-{o}' customer_id, '{st}' order_status, "
            f"timestamp'2026-01-{p:02d}' order_purchase_timestamp, "
            + (f"timestamp'2026-01-{d:02d}'" if d else "cast(null as timestamp)") + " order_delivered_customer_date, "
            f"cast(null as timestamp) order_estimated_delivery_date, timestamp'2026-02-{u:02d}' updated_at, "
            "current_timestamp() ingestion_ts, 'b' batch_id"
            for o, st, p, d, u in rows)
        return spark.sql(sql)

    def bronze_write(rows):
        write_bronze(spark, orders(rows), bpath, cfg, delta_schema(spark, bpath))

    def silver_run(force=False):
        a = Step()
        process_incremental(spark, a, "orders", SPECS["orders"], store, force)
        silver = {r["order_id"]: str(r["updated_at"])[:10] for r in spark.read.format("delta").load(spath).collect()}
        return {"read": a.source_rows, "ins": a.inserted, "upd": a.updated, "rej": a.rejected,
                "detail": a.details[0] if a.details else None, "silver": silver,   # Silver lineage comes first
                "report": getattr(a, "rejected_report", None),
                "silver_version": DeltaTable.forPath(spark, spath).history(1).first()["version"],
                "checkpoint": store.get("orders")}

    # 1. first run: no checkpoint -> full pass
    bronze_write([("o1", "delivered", 1, 5, 1), ("o2", "shipped", 1, None, 10)])
    out["first"] = silver_run()
    # 2. nothing new in Bronze -> skipped, Silver not rewritten
    out["skip"] = silver_run()
    # 3. new Bronze versions: newer valid (o1), LATE older (o2 day 5 < 10), new key (o3), invalid new key (o4),
    #    invalid NEWER version of o2 (delivered before purchase)
    bronze_write([("o1", "delivered", 1, 5, 2), ("o2", "processing", 1, None, 5), ("o3", "approved", 3, None, 3),
                  ("o4", "lost", 4, None, 4), ("o2", "delivered", 9, 2, 20)])
    out["incremental"] = silver_run()
    q = spark.read.format("delta").load(quarantine_path("orders"))
    out["quarantine"] = sorted([r["order_id"], r["reason"]] for r in q.collect())
    # 4. rerun -> skip; 5. forced full refresh -> same Silver, quarantine not duplicated
    out["rerun"] = silver_run()
    out["full"] = silver_run(force=True)
    out["quarantine_rows_after_full"] = spark.read.format("delta").load(quarantine_path("orders")).count()
    # 6. a delete in Bronze (simulated data loss) is not propagated to Silver
    DeltaTable.forPath(spark, bpath).delete("order_id = 'o3'")
    out["after_delete"] = silver_run()
    spark.stop()
    print("RESULT " + json.dumps(out, default=str))
''')


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    env = {**os.environ, "LAKE_PATH": str(tmp_path_factory.mktemp("lake")), "PYTHONPATH": str(SRC)}
    proc = subprocess.run([sys.executable, "-c", SCRIPT], cwd=SRC, env=env, capture_output=True, text=True,
                          timeout=900)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")]
    assert proc.returncode == 0 and lines, f"script failed:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    return json.loads(lines[-1][len("RESULT "):])


def test_first_run_is_full(result):
    r = result["first"]
    assert (r["read"], r["ins"], r["rej"]) == (2, 2, 0) and r["detail"].startswith("mode=full")
    assert r["checkpoint"] is not None


def test_no_new_bronze_versions_skips_without_rewrite(result):
    r = result["skip"]
    assert (r["read"], r["ins"], r["upd"]) == (0, 0, 0)
    assert r["silver_version"] == result["first"]["silver_version"]


def test_incremental_reads_only_new_rows(result):
    r = result["incremental"]
    assert r["read"] == 5 and r["detail"].startswith("mode=incremental")
    assert (r["ins"], r["upd"], r["rej"]) == (1, 1, 2)            # o3 inserted, o1 updated, o4 + o2(v20) rejected
    assert "late_ignored=1" in r["detail"] and "superseded_by_invalid=1" in r["detail"]


def test_late_and_invalid_versions_do_not_overwrite_silver(result):
    silver = result["incremental"]["silver"]
    assert silver == {"o1": "2026-02-02", "o2": "2026-02-10", "o3": "2026-02-03"}   # o2 keeps last valid


def test_quarantine_lists_rules_failed(result):
    assert result["quarantine"] == [["o2", "delivered_after_purchase"], ["o4", "accepted:order_status"]]


def test_rejected_records_report_per_rule_with_samples(result):
    table, counts, samples = result["incremental"]["report"]
    assert table == "silver.orders"
    assert counts == {"delivered_after_purchase": 1, "accepted:order_status": 1}
    assert samples == {"delivered_after_purchase": "order_id=o2", "accepted:order_status": "order_id=o4"}


def test_rerun_and_full_refresh_are_idempotent(result):
    assert (result["rerun"]["read"], result["rerun"]["ins"], result["rerun"]["upd"]) == (0, 0, 0)
    full = result["full"]
    assert full["detail"].startswith("mode=full") and (full["ins"], full["upd"]) == (0, 0)
    assert full["silver"] == result["incremental"]["silver"]
    assert result["quarantine_rows_after_full"] == 2


def test_bronze_delete_is_not_propagated(result):
    r = result["after_delete"]
    assert r["read"] == 0 and r["detail"].startswith("mode=incremental") and "o3" in r["silver"]
