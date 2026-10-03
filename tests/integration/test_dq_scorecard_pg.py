"""Integration test of the data quality scorecard views against the warehouse Postgres.
Writes rows under a unique pytest run id and deletes only those rows afterwards."""
import uuid

import pytest

sa = pytest.importorskip("sqlalchemy")
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def eng():
    from common import get_engine
    from migrate import apply_migrations

    e = get_engine()
    try:
        with e.connect() as c:
            c.execute(sa.text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"Postgres not reachable: {str(exc).splitlines()[0]}")
    apply_migrations(e)
    yield e
    e.dispose()


@pytest.fixture
def run_id(eng):
    rid = f"pytest-{uuid.uuid4().hex[:12]}"
    yield rid
    with eng.begin() as c:
        c.execute(sa.text("DELETE FROM audit.dq_log WHERE run_id = :r"), {"r": rid})


def _insert(eng, run_id, rows):
    from dbt_results import insert_rows
    insert_rows(eng, [{"run_id": run_id, "name": n, "status": s, "failed": 0, "detail": "", "source": "checks",
                       "category": cat} for n, s, cat in rows])


def test_scorecard_scores_a_run(eng, run_id):
    _insert(eng, run_id, [("a", "PASS", "uniqueness"), ("b", "PASS", "uniqueness"), ("c", "WARN", "freshness"),
                          ("d", "FAIL", "reconciliation")])
    with eng.connect() as c:
        row = c.execute(sa.text("SELECT checks, passed, warned, failed, pass_rate, health_score "
                                "FROM audit.dq_scorecard WHERE run_id = :r"), {"r": run_id}).one()
        cats = dict(c.execute(sa.text("SELECT category, pass_rate FROM audit.dq_scorecard_by_category "
                                      "WHERE run_id = :r"), {"r": run_id}).fetchall())
    assert tuple(row) == (4, 2, 1, 1, 50.0, 62.5)
    assert {k: float(v) for k, v in cats.items()} == {"uniqueness": 100.0, "freshness": 0.0, "reconciliation": 0.0}


def test_retried_check_counts_once_with_latest_result(eng, run_id):
    _insert(eng, run_id, [("dbt:x", "FAIL", "validity")])
    _insert(eng, run_id, [("dbt:x", "PASS", "validity")])      # dbt retry recorded the same test again
    with eng.connect() as c:
        row = c.execute(sa.text("SELECT checks, passed, failed FROM audit.dq_scorecard WHERE run_id = :r"),
                        {"r": run_id}).one()
    assert tuple(row) == (1, 1, 0)
