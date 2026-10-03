"""dbt performance benchmark on a COPY of the warehouse: full refresh vs incremental facts, with and without
indexes. Real schemas are only read.

For each scale, the staging tables are copied (N copies of every order with new keys) into the schema
bench_staging, and the models are built into analytics_bench* (DBT_SOURCE_SCHEMA / DBT_SCHEMA). Both are
dropped at the end. Timed: dbt's own model execution time (run_results.json, without dbt start-up) and the
wall-clock time of the whole `dbt run`.

  python benchmark_dbt.py --label baseline [--scales 1 10] [--repeat 3]
"""
import argparse
import json
import os
import subprocess
import time

import sqlalchemy as sa

from benchmark import Recorder
from common import get_engine, get_logger

log = get_logger("benchmark_dbt")
DBT = os.getenv("DBT_BIN", "/opt/dbt_venv/bin/dbt")
PROJECT = os.getenv("DBT_PROJECT_DIR", "/app/dbt_project")
TARGET = "/tmp/dbt/bench_target"
SRC_SCHEMA, OUT_SCHEMA = "bench_staging", "analytics_bench"
TABLES = ("orders", "customers", "products", "sellers", "order_items", "order_payments")
FACTS = "fact_orders fact_payments"
STAGING_INDEXES = ("orders (order_id)", "order_items (order_id)", "order_payments (order_id)")


def dbt(args, fact_indexes):
    env = {**os.environ, "DBT_SOURCE_SCHEMA": SRC_SCHEMA, "DBT_SCHEMA": OUT_SCHEMA, "DBT_TARGET_PATH": TARGET,
           "DBT_LOG_PATH": "/tmp/dbt/bench_logs"}
    cmd = [DBT, "run", *args, "--vars", json.dumps({"fact_indexes": fact_indexes})]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=PROJECT, env=env, capture_output=True, text=True)
    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        raise RuntimeError(f"dbt failed:\n{proc.stdout[-3000:]}")
    results = json.load(open(f"{TARGET}/run_results.json"))["results"]
    model_s = sum(r["execution_time"] for r in results if r["unique_id"].split(".")[-1] in FACTS.split())
    return model_s, wall


def copy_staging(eng, scale, staging_indexes):
    with eng.begin() as c:
        c.execute(sa.text(f"DROP SCHEMA IF EXISTS {SRC_SCHEMA} CASCADE"))
        c.execute(sa.text(f"CREATE SCHEMA {SRC_SCHEMA}"))
        for t in TABLES:
            c.execute(sa.text(f"CREATE TABLE {SRC_SCHEMA}.{t} AS SELECT * FROM staging.{t}"))
        for i in range(1, scale):   # extra copies of the order-keyed tables with new keys
            for t in ("orders", "order_items", "order_payments"):
                cols = [r[0] for r in c.execute(sa.text(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='staging' "
                    "AND table_name=:t ORDER BY ordinal_position"), {"t": t})]
                sel = ", ".join(f"order_id || '_x{i}'" if col == "order_id" else f'"{col}"' for col in cols)
                c.execute(sa.text(f"INSERT INTO {SRC_SCHEMA}.{t} SELECT {sel} FROM staging.{t}"))
        if staging_indexes:
            for ix in STAGING_INDEXES:
                c.execute(sa.text(f"CREATE INDEX ON {SRC_SCHEMA}.{ix}"))
        for t in TABLES:
            c.execute(sa.text(f"ANALYZE {SRC_SCHEMA}.{t}"))
        return c.execute(sa.text(f"SELECT count(*) FROM {SRC_SCHEMA}.order_items")).scalar()


def change_orders(eng, fraction, seed):
    """A day of changes: `fraction` of orders get a newer updated_at (and a new status)."""
    with eng.begin() as c:
        c.execute(sa.text("SELECT setseed(:s)"), {"s": seed})
        return c.execute(sa.text(
            f"UPDATE {SRC_SCHEMA}.orders SET updated_at = updated_at + interval '1 day' + "
            f"(random() * interval '1 hour'), order_status = 'delivered' "
            f"WHERE random() < :f"), {"f": fraction}).rowcount


def drop_outputs(eng):
    with eng.begin() as c:
        for s in (OUT_SCHEMA, f"{OUT_SCHEMA}_stg", f"{OUT_SCHEMA}_int", SRC_SCHEMA):
            c.execute(sa.text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--scales", type=int, nargs="+", default=[1, 10])
    ap.add_argument("--repeat", type=int, default=3)
    a = ap.parse_args()
    eng = get_engine()
    rec = Recorder(eng, a.label)
    try:
        for scale in a.scales:
            for indexes in (False, True):
                tag = "with indexes" if indexes else "no indexes"
                drop_outputs(eng)
                items = copy_staging(eng, scale, indexes)
                dbt(["--select", f"+{FACTS.split()[0]}", f"+{FACTS.split()[1]}"], indexes)   # build parents once

                full = [dbt(["--full-refresh", "--select", *FACTS.split()], indexes) for _ in range(a.repeat)]
                rec.record("dbt_facts", f"full refresh, {tag}", scale, items, [m for m, _ in full],
                           wall_seconds=[round(w, 1) for _, w in full])
                none = [dbt(["--select", *FACTS.split()], indexes) for _ in range(a.repeat)]
                rec.record("dbt_facts", f"incremental no changes, {tag}", scale, items, [m for m, _ in none],
                           wall_seconds=[round(w, 1) for _, w in none])
                inc, changed = [], []
                for i in range(a.repeat):
                    changed.append(change_orders(eng, 0.01, 0.1 + i / 10))
                    inc.append(dbt(["--select", *FACTS.split()], indexes))
                rec.record("dbt_facts", f"incremental 1% changed, {tag}", scale, items, [m for m, _ in inc],
                           wall_seconds=[round(w, 1) for _, w in inc], orders_changed=changed)
    finally:
        drop_outputs(eng)
        eng.dispose()


if __name__ == "__main__":
    main()
