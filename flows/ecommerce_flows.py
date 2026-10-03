"""Prefect flows for the e-commerce pipeline.

ecommerce-daily     scheduled (see serve.py): extract -> silver -> staging -> dbt -> quality checks
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

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run, task_run

SRC = os.getenv("SRC_DIR", "/app/src")
PY = os.getenv("PIPELINE_PYTHON", "/opt/venv/bin/python")
sys.path.insert(0, SRC)

from audit import audit_step  # noqa: E402
from windowing import split_window  # noqa: E402

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


def notify_failure(flow, flow_run, state):
    """Flow hook (failed or crashed). Posts to ALERT_WEBHOOK_URL (Slack-compatible) when set."""
    msg = f"PIPELINE FAILURE flow={flow.name} run={flow_run.name} id={flow_run.id} state={state.name}: {state.message}"
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


# ------------------------------------------------------------------- flows
def run_pipeline_steps():
    """The standard load, in order. Shared by the daily flow and the setup flow."""
    extract_bronze()
    transform_silver()
    load_warehouse()
    dbt_build()
    quality_checks()


@flow(name="ecommerce-daily", log_prints=True, on_failure=[notify_failure], on_crashed=[notify_failure])
def daily_pipeline():
    """Incremental daily load: Bronze -> Silver -> Postgres staging -> dbt Gold -> data quality checks."""
    migrate_db()
    try:
        with flow_audit():
            run_pipeline_steps()
    finally:
        publish_run_summary("daily")


@flow(name="ecommerce-backfill", flow_run_name="backfill-{start}-to-{end}", log_prints=True,
      on_failure=[notify_failure], on_crashed=[notify_failure])
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
                transform_silver()
                load_warehouse()
                dbt_build()
                quality_checks()
    finally:
        publish_run_summary("backfill", {"Window": f"[{start}, {end})", "Chunks": len(windows),
                                         "Downstream rebuilt": rebuild_downstream})


if __name__ == "__main__":
    daily_pipeline()   # quick local run without the scheduler: python flows/ecommerce_flows.py
