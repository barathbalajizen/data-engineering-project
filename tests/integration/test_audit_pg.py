"""Integration tests against the real warehouse (the docker compose Postgres).

Run inside the stack:  docker compose exec pipeline pytest tests -m integration -v
Skipped automatically when Postgres is not reachable (e.g. in the unit-test CI job).
Only rows created by these tests are deleted; migrations are additive and idempotent.
"""
import uuid

import pytest

sa = pytest.importorskip("sqlalchemy")
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def eng():
    from common import get_engine

    e = get_engine()
    try:
        with e.connect() as c:
            c.execute(sa.text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"Postgres not reachable: {str(exc).splitlines()[0]}")
    yield e
    e.dispose()


@pytest.fixture
def run_id(eng):
    rid = f"pytest-{uuid.uuid4().hex[:12]}"
    yield rid
    with eng.begin() as c:
        c.execute(sa.text("DELETE FROM audit.pipeline_run_log WHERE pipeline_run_id = :r"), {"r": rid})
        c.execute(sa.text("DELETE FROM audit.lineage WHERE pipeline_run_id = :r"), {"r": rid})


def test_migrations_are_idempotent(eng):
    from migrate import apply_migrations

    apply_migrations(eng)                      # brings the database up to date
    assert apply_migrations(eng) == []         # second call has nothing to do
    with eng.connect() as c:
        tables = {r[0] for r in c.execute(sa.text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'audit'"))}
    assert {"pipeline_run_log", "lineage", "schema_migrations", "dq_log"} <= tables


def test_audit_step_writes_success_row_and_lineage(eng, run_id):
    from audit import audit_step

    with audit_step("extract-bronze", table_name="bronze.orders", batch_id="b-1", run_id=run_id,
                    attempt=2, engine=eng) as a:
        a.source_rows, a.inserted, a.updated, a.rejected = 100, 90, 5, 5
        a.lineage("postgres:source.orders", "delta:bronze/orders", 90, 7, "test")
    with eng.connect() as c:
        row = c.execute(sa.text(
            "SELECT status, retry_count, source_row_count, inserted_count, updated_count, rejected_count, "
            "end_ts >= start_ts, duration_seconds >= 0 FROM audit.pipeline_run_log WHERE pipeline_run_id = :r"),
            {"r": run_id}).one()
        lin = c.execute(sa.text("SELECT batch_id, row_count, target_version FROM audit.lineage "
                                "WHERE pipeline_run_id = :r"), {"r": run_id}).one()
    assert tuple(row) == ("SUCCESS", 1, 100, 90, 5, 5, True, True)
    assert tuple(lin) == ("b-1", 90, 7)


def test_audit_step_writes_failed_row(eng, run_id):
    from audit import audit_step

    with pytest.raises(RuntimeError):
        with audit_step("dbt-build", level="task", run_id=run_id, engine=eng):
            raise RuntimeError("dbt test failed")
    with eng.connect() as c:
        status, err = c.execute(sa.text("SELECT status, error_details FROM audit.pipeline_run_log "
                                        "WHERE pipeline_run_id = :r"), {"r": run_id}).one()
    assert status == "FAILED" and "dbt test failed" in err


def test_close_stale_marks_running_rows_abandoned(eng, run_id):
    from migrate import close_stale_runs

    with eng.begin() as c:
        c.execute(sa.text("INSERT INTO audit.pipeline_run_log (pipeline_run_id, task_name, level, status) "
                          "VALUES (:r, 'extract-bronze', 'task', 'RUNNING')"), {"r": run_id})
    assert close_stale_runs(eng, only_run_id=run_id) == 1
    with eng.connect() as c:
        status = c.execute(sa.text("SELECT status FROM audit.pipeline_run_log WHERE pipeline_run_id = :r"),
                           {"r": run_id}).scalar()
    assert status == "ABANDONED"
