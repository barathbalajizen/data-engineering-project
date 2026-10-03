"""Prefect flows for the e-commerce pipeline.

ecommerce-daily     scheduled (see serve.py): extract -> silver -> staging -> dbt -> quality checks
                    on demand: start_from / stop_after run part of it; resume_failed=true continues the latest
                    failed daily run from the step that failed (found in the audit log)
ecommerce-backfill  manual, parameterised: re-extract [start, end) in chunks, then rebuild downstream

Each step runs the existing script in a subprocess (Spark/JVM isolation, scripts stay runnable
standalone). Its output is streamed into the Prefect run logs. A non-zero exit fails the task, and
Prefect retries it with backoff. Every script is idempotent, so retries and reruns are safe.

Audit: each flow run and each task attempt gets a row in audit.pipeline_run_log (status, duration, retry
count, error); the scripts add one row per table with row counts. Migrations run at the start of each flow.
"""
import collections
import contextlib
import json
import logging
import os
import subprocess
import sys
import threading
import urllib.request
from typing import Literal

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run, task_run

SRC = os.getenv("SRC_DIR", "/app/src")
PY = os.getenv("PIPELINE_PYTHON", "/opt/venv/bin/python")
sys.path.insert(0, SRC)

from audit import audit_step  # noqa: E402
from pipeline_plan import STEPS, plan_steps, resume_point  # noqa: E402
from windowing import split_window  # noqa: E402

# Shown as a dropdown in the Prefect UI (Run > Custom run)
StepName = Literal["extract-bronze", "transform-silver", "load-warehouse", "dbt-build", "quality-checks"]

# 3 retries, waiting 1, 2 then 4 minutes
RETRIES = dict(retries=3, retry_delay_seconds=[60, 120, 240])


# ----------------------------------------------------------------- helpers
def run_context():
    """Ids of the current flow run / task attempt, passed to the scripts as environment variables."""
    return {"RUN_ID": flow_run.id or "manual", "FLOW_NAME": flow_run.flow_name or "",
            "TASK_NAME": task_run.task_name or "", "ATTEMPT": str(task_run.run_count or 1)}


def flow_audit():
    """Audit row for the whole flow run (level='flow')."""
    return audit_step(task_name=flow_run.flow_name, level="flow", run_id=flow_run.id,
                      flow_name=flow_run.flow_name, attempt=flow_run.run_count or 1)


def run_cmd(cmd, timeout=3600, audit=True):
    """Run a command, stream its output into the Prefect logs, raise if it fails or times out.
    With audit=True the attempt is recorded in audit.pipeline_run_log (level='task')."""
    ctx = run_context()
    step = (audit_step(task_name=ctx["TASK_NAME"] or os.path.basename(cmd[1]), level="task", run_id=ctx["RUN_ID"],
                       flow_name=ctx["FLOW_NAME"], attempt=int(ctx["ATTEMPT"]))
            if audit else contextlib.nullcontext())
    with step:
        _run_streaming(cmd, timeout, {**os.environ, **ctx})


def _run_streaming(cmd, timeout, env):
    log = get_run_logger()
    log.info("$ %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, bufsize=1)
    timed_out = threading.Event()

    def _kill():
        timed_out.set()
        proc.kill()

    timer = threading.Timer(timeout, _kill)
    timer.start()
    tail = collections.deque(maxlen=30)
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log.info(line)
                tail.append(line)
        rc = proc.wait()
    finally:
        timer.cancel()
    if timed_out.is_set():
        raise TimeoutError(f"{os.path.basename(cmd[1])} exceeded {timeout}s and was killed")
    if rc != 0:
        raise RuntimeError(f"{os.path.basename(cmd[1])} exited with code {rc}. Last output:\n" + "\n".join(tail))


def failed_step_hint(run_id):
    """'failed at <step>; resume with ...' from the audit log (best effort, never raises)."""
    try:
        import sqlalchemy as sa
        from common import get_engine

        eng = get_engine()
        try:
            with eng.connect() as c:
                rows = c.execute(sa.text("SELECT task_name, status FROM audit.pipeline_run_log "
                                         "WHERE pipeline_run_id = :r AND level = 'task' ORDER BY id"),
                                 {"r": str(run_id)}).fetchall()
        finally:
            eng.dispose()
        step = resume_point([tuple(r) for r in rows])
        return f" | failed at {step}; resume: run ecommerce-daily with resume_failed=true" if step else ""
    except Exception:
        return ""


def notify_failure(flow, flow_run, state):
    """Flow hook (failed, crashed or cancelled). Posts to ALERT_WEBHOOK_URL (Slack-compatible) when set."""
    msg = (f"PIPELINE FAILURE flow={flow.name} run={flow_run.name} id={flow_run.id} state={state.name}: "
           f"{state.message}{failed_step_hint(flow_run.id) if flow.name == 'ecommerce-daily' else ''}")
    logging.getLogger("pipeline.alert").error(msg)
    url = os.getenv("ALERT_WEBHOOK_URL")
    if url:
        try:
            req = urllib.request.Request(url, data=json.dumps({"text": msg}).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            logging.getLogger("pipeline.alert").exception("alert webhook failed")


# ------------------------------------------------------------------- tasks
@task(name="migrate-db", retries=2, retry_delay_seconds=30)
def migrate_db():
    """Apply pending sql/migrations and close audit rows left RUNNING by a crashed run. Not audited itself:
    on a fresh database the audit table does not exist until this has run."""
    run_cmd([PY, f"{SRC}/migrate.py", "--close-stale"], timeout=600, audit=False)


@task(name="extract-bronze", **RETRIES)
def extract_bronze(start: str | None = None, end: str | None = None):
    cmd = [PY, f"{SRC}/extract_bronze.py"]
    if start:
        cmd += ["--start", start]
    if end:
        cmd += ["--end", end]
    run_cmd(cmd)


@task(name="transform-silver", **RETRIES)
def transform_silver():
    run_cmd([PY, f"{SRC}/transform_silver.py"])


@task(name="load-warehouse", **RETRIES)
def load_warehouse():
    run_cmd([PY, f"{SRC}/load_warehouse.py"])


@task(name="dbt-build", **RETRIES)
def dbt_build():
    # dbt_build.sh runs `dbt build` first and `dbt retry` (resume from the failed model) on later attempts
    run_cmd(["bash", f"{SRC}/dbt_build.sh"])


@task(name="quality-checks", retries=0)   # a failed data check is not transient: retrying would not help
def quality_checks():
    run_cmd([PY, f"{SRC}/checks.py"])


@task(name="run-summary", retries=0)
def publish_run_summary(kind: str, details: dict | None = None):
    """Publish a markdown artifact (Artifacts tab in the UI): row counts, watermark, DQ results of this run.
    Never raises: reporting problems must not fail the pipeline."""
    log = get_run_logger()
    try:
        import sqlalchemy as sa
        from common import get_engine
        from prefect.artifacts import create_markdown_artifact

        eng = get_engine()

        def query(sql, **params):
            try:
                with eng.connect() as c:
                    return c.execute(sa.text(sql), params).fetchall()
            except Exception:
                return None   # table may not exist yet if the run failed early

        def scalar(sql):
            rows = query(sql)
            return rows[0][0] if rows else "n/a"

        dq = query("SELECT check_name, status, rows_failed, detail FROM audit.dq_latest WHERE run_id = :r "
                   "ORDER BY status <> 'FAIL', status <> 'WARN', check_name", r=flow_run.id) or []
        score = query("SELECT checks, passed, warned, failed, pass_rate, health_score FROM audit.dq_scorecard "
                      "WHERE run_id = :r", r=flow_run.id)
        facts = [("Flow run", f"{flow_run.name} (`{flow_run.id}`)"), ("Run type", kind)]
        facts += list((details or {}).items())
        facts += [("Orders watermark", scalar("SELECT last_watermark FROM control.watermark WHERE table_name='orders'")),
                  ("source.orders rows", scalar("SELECT count(*) FROM source.orders")),
                  ("staging.orders rows", scalar("SELECT count(*) FROM staging.orders")),
                  ("analytics.fact_orders rows", scalar("SELECT count(*) FROM analytics.fact_orders"))]
        md = f"# {kind.title()} run summary\n\n| | |\n|---|---|\n"
        md += "\n".join(f"| {k} | {v} |" for k, v in facts)
        md += "\n\n## Data quality checks\n\n"
        if score:
            n, p, w, f, rate, health = score[0]
            md += f"**Scorecard:** {p}/{n} passed, {w} warnings, {f} failed (pass rate {rate}%, health {health}%)\n\n"
        if dq:
            md += "| check | status | rows failed | detail |\n|---|---|---|---|\n"
            md += "\n".join(f"| {r[0]} | {r[1]} | {r[2]} | {r[3] or ''} |" for r in dq)
        else:
            md += "_No checks recorded for this run (it stopped before the quality-checks step)._"
        audit = query("SELECT table_name, source_row_count, inserted_count, updated_count, rejected_count, "
                      "duration_seconds, status FROM audit.pipeline_run_log WHERE pipeline_run_id = :r "
                      "AND level = 'table' ORDER BY id", r=flow_run.id) or []
        if audit:
            md += "\n\n## Rows per table (audit.pipeline_run_log)\n\n"
            md += "| table | read | inserted | updated | rejected | seconds | status |\n|---|---|---|---|---|---|---|\n"
            md += "\n".join("| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in audit)
        create_markdown_artifact(key=f"{kind}-run-summary", markdown=md,
                                 description=f"{kind} pipeline run summary")
    except Exception as exc:
        log.warning("could not publish run summary: %s", exc)


@task(name="find-resume-point", retries=2, retry_delay_seconds=10)
def find_resume_point() -> str | None:
    """First step that did not succeed in the latest daily run, or None when that run succeeded."""
    import sqlalchemy as sa
    from common import get_engine

    log = get_run_logger()
    eng = get_engine()
    try:
        with eng.connect() as c:
            last = c.execute(sa.text(
                "SELECT pipeline_run_id, status FROM audit.pipeline_run_log WHERE level = 'flow' "
                "AND task_name = 'ecommerce-daily' AND pipeline_run_id <> :me ORDER BY id DESC LIMIT 1"),
                {"me": str(flow_run.id)}).first()
            if last is None:
                log.info("no previous daily run in the audit log")
                return None
            rows = c.execute(sa.text("SELECT task_name, status FROM audit.pipeline_run_log "
                                     "WHERE pipeline_run_id = :r AND level = 'task' ORDER BY id"),
                             {"r": last[0]}).fetchall()
    finally:
        eng.dispose()
    if last[1] == "SUCCESS":
        log.info("latest daily run %s succeeded: nothing to resume", last[0])
        return None
    step = resume_point([tuple(r) for r in rows])
    log.info("latest daily run %s ended %s; resuming from %s", last[0], last[1], step)
    return step


# ------------------------------------------------------------------- flows
def step_tasks():
    """Pipeline step name -> Prefect task (looked up at call time). The order is pipeline_plan.STEPS."""
    return {"extract-bronze": extract_bronze, "transform-silver": transform_silver,
            "load-warehouse": load_warehouse, "dbt-build": dbt_build, "quality-checks": quality_checks}


def run_pipeline_steps(steps=STEPS):
    """Run pipeline steps in order; each one starts only after the previous one succeeded (a failure stops
    the run). Shared by the daily flow and the setup flow."""
    tasks = step_tasks()
    for name in steps:
        tasks[name]()


HOOKS = dict(on_failure=[notify_failure], on_crashed=[notify_failure], on_cancellation=[notify_failure])


@flow(name="ecommerce-daily", log_prints=True, **HOOKS)
def daily_pipeline(start_from: StepName | None = None, stop_after: StepName | None = None,
                   resume_failed: bool = False):
    """Incremental daily load: Bronze -> Silver -> Postgres staging -> dbt Gold -> data quality checks.

    start_from / stop_after: run only part of the pipeline (every step is idempotent, so any start is safe).
    resume_failed: continue the latest failed daily run from the step where it failed; does nothing when
    the latest daily run succeeded.
    """
    log = get_run_logger()
    if resume_failed and start_from:
        raise ValueError("use either start_from or resume_failed, not both")
    migrate_db()   # also marks rows of a crashed run ABANDONED, so resume can see where it stopped
    if resume_failed:
        start_from = find_resume_point()
        if start_from is None:
            return
    steps = plan_steps(start_from, stop_after)
    log.info("steps: %s", " > ".join(steps))
    try:
        with flow_audit():
            run_pipeline_steps(steps)
    finally:
        publish_run_summary("daily", {"Steps": " > ".join(steps)} if len(steps) < len(STEPS) else None)


@flow(name="ecommerce-backfill", flow_run_name="backfill-{start}-to-{end}", log_prints=True, **HOOKS)
def backfill_pipeline(start: str, end: str, chunk_days: int = 31, rebuild_downstream: bool = True):
    """Manual backfill of orders whose updated_at is in [start, end).

    The window is split into chunks of `chunk_days`, each its own task with its own retries. Bronze inserts
    only versions it does not have yet, and the daily watermark is left untouched, so this is safe to rerun.
    Set rebuild_downstream=False to extract many windows first and rebuild Silver/Gold once at the end.
    """
    log = get_run_logger()
    windows = split_window(start, end, chunk_days)   # validates the dates up front
    log.info("Backfilling %d window(s) between %s and %s", len(windows), start, end)
    migrate_db()
    try:
        with flow_audit():
            for i, (lo, hi) in enumerate(windows, 1):
                extract_bronze.with_options(
                    task_run_name=f"extract {lo[:10]} to {hi[:10]} ({i}/{len(windows)})")(lo, hi)
            if rebuild_downstream:
                run_pipeline_steps(plan_steps(start_from="transform-silver"))
    finally:
        publish_run_summary("backfill", {"Window": f"[{start}, {end})", "Chunks": len(windows),
                                         "Downstream rebuilt": rebuild_downstream})


if __name__ == "__main__":
    daily_pipeline()   # quick local run without the scheduler: python flows/ecommerce_flows.py
