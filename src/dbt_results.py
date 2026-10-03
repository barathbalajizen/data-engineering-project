"""Record dbt test and source-freshness results in audit.dq_log, next to the pipeline's own checks, so one
data quality scorecard covers both (audit.dq_scorecard).

  python dbt_results.py tests       reads <DBT_TARGET_PATH>/run_results.json  (after dbt build / dbt retry)
  python dbt_results.py freshness   reads <DBT_TARGET_PATH>/sources.json      (after dbt source freshness)

A retried run records the retried tests again under the same run id; the scorecard keeps the latest row
per check. Parsing is pure and unit-tested.
"""
import json
import os
import sys
from pathlib import Path

TARGET = Path(os.getenv("DBT_TARGET_PATH", "/app/dbt_project/target"))
GENERIC = ("not_null", "unique_combination", "unique", "relationships", "accepted_values", "non_negative")
CATEGORY = {"not_null": "completeness", "unique": "uniqueness", "unique_combination": "uniqueness",
            "relationships": "referential_integrity", "accepted_values": "validity", "non_negative": "validity"}
STATUS = {"pass": "PASS", "warn": "WARN", "fail": "FAIL", "error": "FAIL", "runtime error": "FAIL"}


def short_test_name(unique_id):
    """'test.ecommerce.not_null_fact_orders_price.1a2b' -> 'not_null_fact_orders_price'."""
    parts = unique_id.split(".")
    return parts[2] if len(parts) > 2 else unique_id


def category_of(name):
    """Category from the generic test name; anything else is a singular (custom SQL) test."""
    if name.startswith("source_"):
        name = name[len("source_"):]
    for g in GENERIC:   # unique_combination is checked before unique
        if name.startswith(g + "_"):
            return CATEGORY[g]
    return "business_rule"


def parse_run_results(doc, run_id):
    """dq_log rows for the tests in a run_results.json document (models and snapshots are skipped)."""
    rows = []
    for r in doc.get("results", []):
        uid = r.get("unique_id", "")
        status = STATUS.get(str(r.get("status", "")).lower())
        if not uid.startswith("test.") or status is None:   # skipped tests are not results
            continue
        name = short_test_name(uid)
        rows.append({"run_id": run_id, "name": f"dbt:{name}", "status": status,
                     "failed": int(r.get("failures") or 0),
                     "detail": (r.get("message") or "")[:500], "source": "dbt", "category": category_of(name)})
    return rows


def parse_freshness(doc, run_id):
    """dq_log rows for a sources.json document (dbt source freshness)."""
    rows = []
    for r in doc.get("results", []):
        uid = r.get("unique_id", "")                      # source.ecommerce.staging.orders
        name = ".".join(uid.split(".")[2:]) or uid
        status = STATUS.get(str(r.get("status", "")).lower(), "FAIL")
        age_h = r.get("max_loaded_at_time_ago_in_s")
        detail = f"max_loaded_at={r.get('max_loaded_at')} age_hours={age_h / 3600:.1f}" if age_h is not None \
            else (r.get("error") or "")[:500]
        rows.append({"run_id": run_id, "name": f"dbt_freshness:{name}", "status": status, "failed": 0,
                     "detail": detail, "source": "dbt", "category": "freshness"})
    return rows


def insert_rows(eng, rows):
    import sqlalchemy as sa
    with eng.begin() as c:
        for r in rows:
            c.execute(sa.text("INSERT INTO audit.dq_log (run_id, check_name, status, rows_failed, detail, "
                              "check_source, category) VALUES (:run_id, :name, :status, :failed, :detail, "
                              ":source, :category)"), r)


def main(kind):
    from common import RUN_ID, get_engine, get_logger

    log = get_logger("dbt_results")
    path, parse = {"tests": (TARGET / "run_results.json", parse_run_results),
                   "freshness": (TARGET / "sources.json", parse_freshness)}[kind]
    if not path.exists():
        log.warning("%s not found, nothing recorded", path)
        return
    rows = parse(json.loads(path.read_text(encoding="utf-8")), RUN_ID)
    eng = get_engine()
    try:
        insert_rows(eng, rows)
    finally:
        eng.dispose()
    counts = {s: sum(r["status"] == s for r in rows) for s in ("PASS", "WARN", "FAIL")}
    log.info("recorded %d dbt %s result(s) in audit.dq_log: %s", len(rows), kind, counts)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "tests")
