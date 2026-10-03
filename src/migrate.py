"""Apply sql/migrations/*.sql to the warehouse, in name order, each exactly once.

sql/init.sql only runs when the Postgres volume is created, so schema changes for existing databases go
in numbered migration files instead. Applied versions are recorded in audit.schema_migrations; an advisory
lock makes concurrent callers safe. Migrations must be additive (no data is ever dropped here).

Also marks audit rows left in RUNNING by a crashed run as ABANDONED (call with --close-stale).
Usage: python migrate.py [--close-stale]
"""
import argparse
import os
from pathlib import Path

MIGRATIONS_DIR = Path(os.getenv("MIGRATIONS_DIR", Path(__file__).resolve().parent.parent / "sql" / "migrations"))
LOCK_ID = 7_201_001   # arbitrary constant for pg_advisory_xact_lock


def pending_migrations(files, applied):
    """Return the migration files not applied yet, sorted by name (pure, unit-tested)."""
    return sorted((f for f in files if Path(f).stem not in applied), key=lambda f: Path(f).name)


def apply_migrations(eng, migrations_dir=MIGRATIONS_DIR, log=None):
    import sqlalchemy as sa

    files = sorted(Path(migrations_dir).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no migration files in {migrations_dir}")
    with eng.begin() as c:
        c.execute(sa.text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        c.execute(sa.text("CREATE SCHEMA IF NOT EXISTS audit"))
        c.execute(sa.text("CREATE TABLE IF NOT EXISTS audit.schema_migrations ("
                          "version TEXT PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT now())"))
        applied = {r[0] for r in c.execute(sa.text("SELECT version FROM audit.schema_migrations"))}
        todo = pending_migrations(files, applied)
        for f in todo:
            # Raw driver cursor without parameters (same transaction): with parameters, psycopg2 would read
            # every % in the SQL (e.g. LIKE 'recon_%') as a placeholder
            c.connection.dbapi_connection.cursor().execute(Path(f).read_text(encoding="utf-8"))
            c.execute(sa.text("INSERT INTO audit.schema_migrations(version) VALUES (:v)"), {"v": Path(f).stem})
            if log:
                log.info("applied migration %s", Path(f).name)
    if log:
        log.info("migrations up to date (%d applied now, %d total)", len(todo), len(files))
    return [Path(f).stem for f in todo]


def close_stale_runs(eng, log=None, exclude_run_id=None, only_run_id=None):
    """Mark RUNNING audit rows as ABANDONED. Only call at the start of a flow: the runner executes one flow
    run at a time (serve limit=1), so anything still RUNNING then (except the current run) was left behind
    by a crash. only_run_id limits it to one run (used by tests)."""
    import sqlalchemy as sa

    sql = ("UPDATE audit.pipeline_run_log SET status='ABANDONED', end_ts=clock_timestamp(), "
           "error_details=coalesce(error_details, 'process ended without closing this row (crash or kill)') "
           "WHERE status='RUNNING' AND pipeline_run_id IS DISTINCT FROM :exclude")
    params = {"exclude": exclude_run_id}
    if only_run_id:
        sql += " AND pipeline_run_id = :only"
        params["only"] = only_run_id
    with eng.begin() as c:
        n = c.execute(sa.text(sql), params).rowcount
    if log and n:
        log.warning("marked %d stale RUNNING audit row(s) as ABANDONED", n)
    return n


def main(close_stale=False):
    from common import get_engine, get_logger

    log, eng = get_logger("migrate"), get_engine()
    try:
        apply_migrations(eng, log=log)
        if close_stale:
            close_stale_runs(eng, log=log, exclude_run_id=os.getenv("RUN_ID"))
    finally:
        eng.dispose()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--close-stale", action="store_true")
    main(ap.parse_args().close_stale)
