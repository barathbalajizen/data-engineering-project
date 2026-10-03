"""Integration test of the real Bronze write path (schema drift, evolution, CDF, time travel) on Delta Lake.

Runs extract_bronze.check_schema / write_bronze against a throwaway lake in a temp folder, in a subprocess
so it gets its own Delta-enabled Spark session (the unit tests' plain session cannot load Delta).
Needs delta-spark (the pipeline container):  docker compose exec pipeline pytest tests -m integration -v
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
    from pyspark.sql import functions as F
    from common import get_spark, bronze_path
    from delta_utils import read_as_of, read_changes, table_properties
    from extract_bronze import check_schema, write_bronze
    from schema_drift import SchemaDriftError

    class Recorder:                      # stands in for audit_step: collects schema changes
        def __init__(self): self.changes = []
        def schema_changes(self, table, rows): self.changes += [list(r) for r in rows]

    spark = get_spark("test_bronze")
    out = {}
    inc = {"mode": "incremental", "version_keys": ["order_id", "updated_at"], "cdf": True}
    full = {"mode": "full"}
    path = bronze_path("orders")

    def bronze(df):                      # metadata the extract adds
        return df.withColumn("ingestion_ts", F.current_timestamp()).withColumn("batch_id", F.lit("b"))

    def load(table, df, cfg):
        rec = Recorder()
        aligned, existing = check_schema(spark, rec, table, df)
        ins, ver = write_bronze(spark, bronze(aligned), bronze_path(table), cfg, existing)
        return rec.changes, ins, ver

    # 1. first load creates the table and sets CDF + retention properties
    out["first"] = load("orders", spark.sql(
        "select 'o1' order_id, cast(1 as bigint) qty, timestamp'2026-01-01' updated_at"), inc)[1:]
    out["props"] = {k: v for k, v in table_properties(spark, path).items() if k.startswith("delta.")}

    # 2. source adds a column -> recorded, table evolves, old rows get NULL
    df = spark.sql("select 'o2' order_id, cast(2 as bigint) qty, timestamp'2026-01-02' updated_at, 'X10' coupon")
    changes, ins, ver = load("orders", df, inc)
    out["added"] = {"changes": changes, "inserted": ins, "version": ver,
                    "columns": spark.read.format("delta").load(path).columns,
                    "null_coupons": spark.read.format("delta").load(path).filter("coupon is null").count()}

    # 3. narrower type (int instead of bigint) -> cast, write succeeds
    df = spark.sql("select 'o3' order_id, cast(3 as int) qty, timestamp'2026-01-03' updated_at, 'Y' coupon")
    changes, ins, _ = load("orders", df, inc)
    out["cast"] = {"changes": changes, "inserted": ins,
                   "qty_type": dict(spark.read.format("delta").load(path).dtypes)["qty"]}

    # 4. rerun of the same rows -> no new version, 0 inserted (idempotent)
    out["rerun"] = load("orders", df, inc)[1:]

    # 5. removed column and incompatible type -> rejected, table untouched
    v_before = spark.sql(f"describe history delta.`{path}`").agg(F.max("version")).first()[0]
    for name, sql in (("removed", "select 'o4' order_id, timestamp'2026-01-04' updated_at, 'Z' coupon"),
                      ("incompatible", "select 'o5' order_id, 'many' qty, timestamp'2026-01-05' updated_at, "
                                       "'Z' coupon")):
        rec = Recorder()
        try:
            check_schema(spark, rec, "orders", spark.sql(sql))
            out[name] = {"raised": False}
        except SchemaDriftError as e:
            out[name] = {"raised": True, "changes": rec.changes, "msg": str(e)}
    out["version_unchanged"] = v_before == spark.sql(
        f"describe history delta.`{path}`").agg(F.max("version")).first()[0]

    # 6. Change Data Feed shows the inserts after the table was created; time travel sees version 0
    cdf = read_changes(spark, path, 1).groupBy("_change_type").count().collect()
    out["cdf"] = {r["_change_type"]: r["count"] for r in cdf}
    from delta_utils import cdf_enabled_since
    out["cdf_since"] = cdf_enabled_since(spark, path)
    out["as_of_v0"] = read_as_of(spark, path, version=0).count()
    out["current"] = spark.read.format("delta").load(path).count()

    # 7. full-snapshot table: added column evolves through overwrite + mergeSchema
    load("products", spark.sql("select 'p1' product_id, 10.0 weight"), full)
    changes, ins, _ = load("products", spark.sql("select 'p1' product_id, 10.0 weight, 'red' colour"), full)
    out["full"] = {"changes": changes, "inserted": ins,
                   "columns": spark.read.format("delta").load(bronze_path("products")).columns}
    spark.stop()
    print("RESULT " + json.dumps(out, default=str))
''')


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    env = {**os.environ, "LAKE_PATH": str(lake), "PYTHONPATH": str(SRC)}
    proc = subprocess.run([sys.executable, "-c", SCRIPT], cwd=SRC, env=env, capture_output=True, text=True,
                          timeout=600)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")]
    assert proc.returncode == 0 and lines, f"script failed:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    return json.loads(lines[-1][len("RESULT "):])


def test_first_load_sets_cdf_and_retention(result):
    assert result["first"][0] == 1
    assert result["props"]["delta.enableChangeDataFeed"] == "true"
    assert result["props"]["delta.logRetentionDuration"] == "interval 30 days"


def test_added_column_evolves_table(result):
    r = result["added"]
    assert r["changes"] == [["added", "coupon", None, "string", "evolved"]]
    assert r["inserted"] == 1 and "coupon" in r["columns"] and r["null_coupons"] == 1


def test_narrower_type_is_cast(result):
    assert result["cast"]["changes"] == [["type_changed", "qty", "bigint", "int", "cast"]]
    assert result["cast"]["inserted"] == 1 and result["cast"]["qty_type"] == "bigint"


def test_rerun_inserts_nothing(result):
    assert result["rerun"][0] == 0


def test_breaking_changes_are_rejected_and_recorded(result):
    assert result["removed"]["raised"] and ["removed", "qty", "bigint", None, "rejected"] in \
        result["removed"]["changes"]
    assert result["incompatible"]["raised"] and \
        ["type_changed", "qty", "bigint", "string", "rejected"] in result["incompatible"]["changes"]
    assert result["version_unchanged"]


def test_change_data_feed_and_time_travel(result):
    assert result["cdf"] == {"insert": 2}           # o2 and o3, written after CDF was on
    assert result["cdf_since"] == 1                 # version 0 = first write, 1 = properties set
    assert result["as_of_v0"] == 1 and result["current"] == 3


def test_full_snapshot_table_evolves(result):
    assert result["full"]["changes"] == [["added", "colour", None, "string", "evolved"]]
    assert "colour" in result["full"]["columns"] and result["full"]["inserted"] == 1
