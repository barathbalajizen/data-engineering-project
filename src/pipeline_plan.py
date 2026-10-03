"""The pipeline's steps, their order (each depends on the one before) and partial-run planning.

Pure Python, unit-tested. The Prefect flows (flows/ecommerce_flows.py) run the steps this module plans:

    plan_steps(start_from="load-warehouse")      -> load-warehouse, dbt-build, quality-checks
    resume_point(task_rows_of_a_failed_run)      -> the first step that did not succeed

Every step is idempotent, so starting from any step is safe: it re-reads what the previous step left.
"""
STEPS = ("extract-bronze", "transform-silver", "load-warehouse", "dbt-build", "quality-checks")

# What each step needs from the step before it (documentation for the UI and the README)
DEPENDS_ON = {
    "extract-bronze": None,                 # source Postgres + control.watermark
    "transform-silver": "extract-bronze",   # Bronze Delta tables
    "load-warehouse": "transform-silver",   # Silver Delta tables
    "dbt-build": "load-warehouse",          # Postgres staging schema
    "quality-checks": "dbt-build",          # all layers
}


def plan_steps(start_from=None, stop_after=None, steps=STEPS):
    """Steps to run, in order, from `start_from` through `stop_after` (both inclusive, default all)."""
    for name, value in (("start_from", start_from), ("stop_after", stop_after)):
        if value is not None and value not in steps:
            raise ValueError(f"{name}={value!r} is not a pipeline step; choose one of {', '.join(steps)}")
    first = steps.index(start_from) if start_from else 0
    last = steps.index(stop_after) if stop_after else len(steps) - 1
    if last < first:
        raise ValueError(f"stop_after={stop_after!r} comes before start_from={start_from!r}")
    return list(steps[first:last + 1])


def resume_point(task_rows, steps=STEPS):
    """Where to resume a failed run, from its task attempts [(task_name, status), ...] in execution order.

    The run's own first step is the first pipeline step it recorded (it may itself have been a partial run).
    From there, the first step whose LATEST attempt is not SUCCESS, or that never ran, is the resume point.
    Returns None when every step from the run's start succeeded (nothing to resume). Non-pipeline tasks
    (migrate-db, run-summary, ...) are ignored. No recorded steps at all: resume from the beginning.
    """
    latest = {}
    for name, status in task_rows:
        if name in steps:
            latest[name] = status
    recorded = [s for s in steps if s in latest]
    if not recorded:
        return steps[0]
    for step in steps[steps.index(recorded[0]):]:
        if latest.get(step) != "SUCCESS":
            return step
    return None
