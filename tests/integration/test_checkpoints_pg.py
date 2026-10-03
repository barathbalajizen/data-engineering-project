"""Integration tests of checkpoint persistence and the execution-history views (warehouse Postgres).
Uses its own pytest rows (table names / run ids) and deletes only those afterwards."""
import uuid
from datetime import datetime

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
def table(eng):
    name = f"pytest_{uuid.uuid4().hex[:10]}"
    with eng.begin() as c:
        c.execute(sa.text("INSERT INTO control.watermark (table_name, last_watermark) VALUES (:t, '2020-01-01')"),
                  {"t": name})
    yield name
    with eng.begin() as c:
        c.execute(sa.text("DELETE FROM control.watermark WHERE table_name = :t"), {"t": name})
        c.execute(sa.text("DELETE FROM control.silver_checkpoint WHERE table_name = :t"), {"t": name})
        c.execute(sa.text("DELETE FROM control.checkpoint_history WHERE table_name = :t"), {"t": name})


def _history(eng, table):
    with eng.connect() as c:
        return [tuple(r) for r in c.execute(sa.text(
            "SELECT checkpoint, old_value, new_value, pipeline_run_id FROM control.checkpoint_history "
            "WHERE table_name = :t ORDER BY id"), {"t": table})]


def test_watermark_only_moves_forward_and_every_move_is_logged(eng, table):
    from checkpoints import advance_watermark, get_watermark

    assert advance_watermark(eng, table, datetime(2021, 1, 1), "run-a") is True
    assert advance_watermark(eng, table, datetime(2020, 6, 1), "run-b") is False    # older: refused
    assert advance_watermark(eng, table, datetime(2021, 1, 1), "run-c") is False    # equal: no change
    assert get_watermark(eng, table) == datetime(2021, 1, 1)
    hist = _history(eng, table)
    assert hist[0][:3] == ("watermark", None, "2020-01-01T00:00:00")                 # the INSERT
    assert hist[1:] == [("watermark", "2020-01-01T00:00:00", "2021-01-01T00:00:00", "run-a")]


def test_silver_checkpoint_changes_are_logged_with_run_id(eng, table):
    from incremental import PgCheckpointStore

    store = PgCheckpointStore(eng, "run-s1")
    store.set(table, 3, "full")
    store.set(table, 3, "full")                       # unchanged: not logged again
    PgCheckpointStore(eng, "run-s2").set(table, 5, "incremental")
    assert store.get(table) == 5
    rows = [r for r in _history(eng, table) if r[0] == "silver_checkpoint"]
    assert rows == [("silver_checkpoint", None, "3", "run-s1"), ("silver_checkpoint", "3", "5", "run-s2")]


@pytest.fixture
def run_id(eng):
    rid = f"pytest-{uuid.uuid4().hex[:12]}"
    yield rid
    with eng.begin() as c:
        c.execute(sa.text("DELETE FROM audit.pipeline_run_log WHERE pipeline_run_id = :r"), {"r": rid})


def test_flow_run_view_summarises_a_run(eng, run_id):
    rows = [("flow", "ecommerce-daily", None, "FAILED", 0, None, None, None),
            ("task", "extract-bronze", None, "SUCCESS", 0, None, None, None),
            ("table", "extract-bronze", "bronze.orders", "SUCCESS", 0, 10, 2, 1),
            ("task", "transform-silver", None, "FAILED", 0, None, None, None),
            ("task", "transform-silver", None, "SUCCESS", 1, None, None, None),
            ("task", "load-warehouse", None, "FAILED", 0, None, None, None)]
    with eng.begin() as c:
        for level, task, table, status, retry, ins, upd, rej in rows:
            c.execute(sa.text(
                "INSERT INTO audit.pipeline_run_log (pipeline_run_id, task_name, level, table_name, status, "
                "retry_count, inserted_count, updated_count, rejected_count, duration_seconds) VALUES "
                "(:r, :t, :l, :tb, :s, :rc, :i, :u, :j, 2)"),
                {"r": run_id, "t": task, "l": level, "tb": table, "s": status, "rc": retry, "i": ins, "u": upd,
                 "j": rej})
    with eng.connect() as c:
        v = c.execute(sa.text("SELECT status, task_attempts, failed_attempts, retried_attempts, first_failed_task, "
                              "steps_succeeded, rows_inserted, rows_updated, rows_rejected FROM audit.v_flow_runs "
                              "WHERE pipeline_run_id = :r"), {"r": run_id}).one()
        stats = c.execute(sa.text("SELECT count(*) FROM audit.v_task_stats WHERE task_name = 'transform-silver' "
                                  "AND attempts >= 2")).scalar()
    assert tuple(v) == ("FAILED", 4, 2, 1, "transform-silver", "extract-bronze > transform-silver", 10, 2, 1)
    assert stats == 1
