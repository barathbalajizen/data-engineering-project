"""End-to-end test of the real ecommerce-daily flow: task order, partial runs and resume after a failure.

Runs the flow on a temporary Prefect server (prefect_test_harness). The step scripts are replaced by a fake
that records what ran and fails on demand, so no data is touched; the audit rows the flow writes go to the
warehouse Postgres (that is what resume reads) and are deleted afterwards.
Needs Prefect + Postgres (the pipeline container):  docker compose exec pipeline pytest tests -m integration
"""
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("prefect")
sa = pytest.importorskip("sqlalchemy")
pytestmark = pytest.mark.integration

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flows"))
SCRIPT_TO_STEP = {"extract_bronze.py": "extract-bronze", "transform_silver.py": "transform-silver",
                  "load_warehouse.py": "load-warehouse", "dbt_build.sh": "dbt-build", "checks.py": "quality-checks"}


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


@pytest.fixture(scope="module")
def harness():
    from prefect.testing.utilities import prefect_test_harness
    with prefect_test_harness(server_startup_timeout=120):
        yield


@pytest.fixture
def flows(monkeypatch, eng, harness):
    import ecommerce_flows as ef

    ran, fail = [], set()

    def fake_run(cmd, timeout, env):
        step = SCRIPT_TO_STEP.get(os.path.basename(cmd[1]))
        if step:
            ran.append(step)
            if step in fail:
                raise RuntimeError(f"simulated failure in {step}")

    monkeypatch.setattr(ef, "_run_streaming", fake_run)
    for name in ("extract_bronze", "transform_silver", "load_warehouse", "dbt_build"):
        monkeypatch.setattr(ef, name, getattr(ef, name).with_options(retries=0))   # fail fast
    run_ids = []

    def run(**params):
        state = ef.daily_pipeline(**params, return_state=True)
        run_ids.append(str(state.state_details.flow_run_id))
        return state

    yield ef, ran, fail, run
    with eng.begin() as c:
        for rid in run_ids:
            c.execute(sa.text("DELETE FROM audit.pipeline_run_log WHERE pipeline_run_id = :r"), {"r": rid})


def test_steps_run_in_dependency_order(flows):
    _, ran, _, run = flows
    assert run().is_completed()
    assert ran == ["extract-bronze", "transform-silver", "load-warehouse", "dbt-build", "quality-checks"]


def test_partial_run(flows):
    _, ran, _, run = flows
    assert run(start_from="load-warehouse", stop_after="dbt-build").is_completed()
    assert ran == ["load-warehouse", "dbt-build"]


def test_failure_stops_the_run_and_resume_continues_from_the_failed_step(flows, eng):
    _, ran, fail, run = flows
    fail.add("load-warehouse")
    failed = run()
    assert failed.is_failed()
    assert ran == ["extract-bronze", "transform-silver", "load-warehouse"]      # dbt never started

    with eng.connect() as c:
        flow_status = c.execute(sa.text("SELECT status FROM audit.pipeline_run_log WHERE level = 'flow' "
                                        "AND pipeline_run_id = :r"),
                                {"r": str(failed.state_details.flow_run_id)}).scalar()
    assert flow_status == "FAILED"

    fail.clear()
    ran.clear()
    assert run(resume_failed=True).is_completed()
    assert ran == ["load-warehouse", "dbt-build", "quality-checks"]             # extract/silver not redone

    ran.clear()
    assert run(resume_failed=True).is_completed()                               # latest run succeeded
    assert ran == []


def test_resume_and_start_from_together_is_rejected(flows):
    _, ran, _, run = flows
    assert run(resume_failed=True, start_from="dbt-build").is_failed() and ran == []
