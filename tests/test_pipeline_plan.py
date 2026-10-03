"""Unit tests for partial-run planning and the resume point."""
import pytest

from pipeline_plan import DEPENDS_ON, STEPS, plan_steps, resume_point


def test_full_plan_is_every_step_in_order():
    assert plan_steps() == list(STEPS)


def test_start_from_and_stop_after_are_inclusive():
    assert plan_steps("load-warehouse") == ["load-warehouse", "dbt-build", "quality-checks"]
    assert plan_steps(stop_after="transform-silver") == ["extract-bronze", "transform-silver"]
    assert plan_steps("dbt-build", "dbt-build") == ["dbt-build"]


@pytest.mark.parametrize("start, stop", [("nope", None), (None, "nope"), ("dbt-build", "extract-bronze")])
def test_invalid_plans_are_rejected(start, stop):
    with pytest.raises(ValueError):
        plan_steps(start, stop)


def test_dependencies_follow_the_step_order():
    assert DEPENDS_ON[STEPS[0]] is None
    assert all(DEPENDS_ON[b] == a for a, b in zip(STEPS, STEPS[1:], strict=False))


def test_resume_from_failed_step():
    rows = [("extract-bronze", "SUCCESS"), ("transform-silver", "SUCCESS"), ("load-warehouse", "FAILED")]
    assert resume_point(rows) == "load-warehouse"


def test_retried_step_that_finally_succeeded_is_done():
    rows = [("extract-bronze", "FAILED"), ("extract-bronze", "SUCCESS"), ("transform-silver", "FAILED"),
            ("transform-silver", "FAILED")]
    assert resume_point(rows) == "transform-silver"


def test_crashed_run_resumes_at_abandoned_step():
    rows = [("extract-bronze", "SUCCESS"), ("transform-silver", "ABANDONED")]
    assert resume_point(rows) == "transform-silver"


def test_step_that_never_started_is_the_resume_point():
    # e.g. the flow was killed between two tasks
    assert resume_point([("extract-bronze", "SUCCESS"), ("transform-silver", "SUCCESS")]) == "load-warehouse"


def test_partial_run_resumes_within_its_own_range():
    # the failed run had start_from=dbt-build: earlier steps are not part of it
    assert resume_point([("dbt-build", "SUCCESS"), ("quality-checks", "FAILED")]) == "quality-checks"


def test_successful_run_has_nothing_to_resume():
    assert resume_point([(s, "SUCCESS") for s in STEPS]) is None


def test_no_recorded_steps_resumes_from_the_start_and_other_tasks_are_ignored():
    assert resume_point([]) == "extract-bronze"
    assert resume_point([("migrate-db", "FAILED")]) == "extract-bronze"
