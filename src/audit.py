"""Pipeline run audit (audit.pipeline_run_log) and lineage (audit.lineage).

    with audit_step("extract-bronze", table_name="bronze.orders", batch_id=bid, engine=eng) as a:
        ...
        a.source_rows, a.inserted, a.updated = n, ins, upd

Writes a RUNNING row on enter and closes it on exit with SUCCESS/FAILED, duration, counts and the error.
Exceptions from the body are never swallowed. Audit writes are best effort: if the audit table cannot be
written (database down, table missing), a warning is logged and the real work still runs, so observability
problems never fail the pipeline. The writer is injectable, so the logic is unit-tested without Postgres.
"""
import logging
import os
import time
import uuid
from datetime import datetime, timezone

log = logging.getLogger("audit")
MAX_ERROR_CHARS = 4000


def make_batch_id(table, now=None):
    """Unique, readable batch id, e.g. orders-20261003T101500-1a2b3c4d."""
    now = now or datetime.now(timezone.utc)
    return f"{table}-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


def merge_counts(metrics):
    """(inserted, updated, deleted) from Delta operationMetrics (values are strings; missing keys -> 0).
    Works for MERGE (numTargetRows*) and plain writes (numOutputRows counts as inserted)."""
    m = {k: int(v) for k, v in (metrics or {}).items() if v is not None and str(v).lstrip("-").isdigit()}
    if "numTargetRowsInserted" in m or "numTargetRowsUpdated" in m:
        return m.get("numTargetRowsInserted", 0), m.get("numTargetRowsUpdated", 0), m.get("numTargetRowsDeleted", 0)
    return m.get("numOutputRows", 0), 0, 0


def delta_last_operation(spark, path):
    """(version, operationMetrics dict) of the latest commit of a Delta table."""
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).select("version", "operationMetrics").first()
    return int(row["version"]), dict(row["operationMetrics"] or {})


def delta_version(spark, path):
    """Current version of a Delta table, or None if it does not exist yet."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return int(DeltaTable.forPath(spark, path).history(1).select("version").first()[0])


def counts_since(version_before, version_after, metrics):
    """(inserted, updated, deleted) written by an operation, given the table version before it and the
    latest commit after it. Delta makes no commit for a no-op MERGE (e.g. insert-only with nothing new),
    so an unchanged version means nothing was written; reading the latest commit's metrics would instead
    report an older operation's numbers."""
    if version_before is not None and version_after == version_before:
        return 0, 0, 0
    return merge_counts(metrics)


def delta_write_counts(spark, path, version_before):
    """(inserted, updated, deleted, version) of the write made since version_before."""
    version, metrics = delta_last_operation(spark, path)
    return (*counts_since(version_before, version, metrics), version)


def _context():
    """Run context passed by the Prefect wrapper (flows/ecommerce_flows.py run_cmd) as environment variables."""
    return {"run_id": os.getenv("RUN_ID", "manual"), "flow_name": os.getenv("FLOW_NAME") or None,
            "task_name": os.getenv("TASK_NAME") or None, "attempt": int(os.getenv("ATTEMPT", "1") or 1)}


class PgAuditWriter:
    """Writes audit rows to Postgres. engine=None creates (and disposes) its own engine."""

    def __init__(self, engine=None):
        self._engine, self._owned = engine, engine is None

    def _eng(self):
        if self._engine is None:
            from common import get_engine
            self._engine = get_engine()
        return self._engine

    def start(self, row):
        import sqlalchemy as sa
        with self._eng().begin() as c:
            return c.execute(sa.text(
                "INSERT INTO audit.pipeline_run_log (pipeline_run_id, flow_name, task_name, level, table_name, "
                "batch_id, status, retry_count) VALUES (:pipeline_run_id, :flow_name, :task_name, :level, "
                ":table_name, :batch_id, 'RUNNING', :retry_count) RETURNING id"), row).scalar()

    def finish(self, row_id, row):
        import sqlalchemy as sa
        with self._eng().begin() as c:
            c.execute(sa.text(
                "UPDATE audit.pipeline_run_log SET end_ts = clock_timestamp(), duration_seconds = :duration, "
                "status = :status, source_row_count = :source_rows, inserted_count = :inserted, "
                "updated_count = :updated, rejected_count = :rejected, error_details = :error WHERE id = :id"),
                {**row, "id": row_id})

    def lineage(self, row):
        import sqlalchemy as sa
        with self._eng().begin() as c:
            c.execute(sa.text(
                "INSERT INTO audit.lineage (pipeline_run_id, task_name, batch_id, source_object, target_object, "
                "row_count, target_version, detail) VALUES (:pipeline_run_id, :task_name, :batch_id, "
                ":source_object, :target_object, :row_count, :target_version, :detail)"), row)

    def schema_change(self, row):
        import sqlalchemy as sa
        with self._eng().begin() as c:
            c.execute(sa.text(
                "INSERT INTO audit.schema_changes (pipeline_run_id, task_name, batch_id, table_name, change_type, "
                "column_name, old_type, new_type, action) VALUES (:pipeline_run_id, :task_name, :batch_id, "
                ":table_name, :change_type, :column_name, :old_type, :new_type, :action)"), row)

    def close(self):
        if self._owned and self._engine is not None:
            self._engine.dispose()
            self._engine = None


class audit_step:  # noqa: N801  (used as a context manager, reads like a function)
    """Context manager that records one audit row. Set source_rows/inserted/updated/rejected inside it."""

    def __init__(self, task_name=None, level="table", table_name=None, batch_id=None, run_id=None,
                 flow_name=None, attempt=None, engine=None, writer=None):
        ctx = _context()
        self.row = {
            "pipeline_run_id": run_id or ctx["run_id"],
            "flow_name": flow_name or ctx["flow_name"],
            "task_name": task_name or ctx["task_name"] or "unknown",
            "level": level,
            "table_name": table_name,
            "batch_id": batch_id,
            "retry_count": max((attempt or ctx["attempt"]) - 1, 0),
        }
        self.writer = writer or PgAuditWriter(engine)
        self.source_rows = self.inserted = self.updated = self.rejected = None
        self.row_id, self._t0 = None, None

    def __enter__(self):
        self._t0 = time.monotonic()
        try:
            self.row_id = self.writer.start(self.row)
        except Exception as exc:
            log.warning("audit start not recorded for %s/%s: %s", self.row["task_name"], self.row["table_name"],
                        _short(exc))
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.row_id is not None:
            result = {
                "duration": round(time.monotonic() - self._t0, 3),
                "status": "FAILED" if exc_type else "SUCCESS",
                "source_rows": self.source_rows, "inserted": self.inserted,
                "updated": self.updated, "rejected": self.rejected,
                "error": f"{exc_type.__name__}: {exc}"[:MAX_ERROR_CHARS] if exc_type else None,
            }
            try:
                self.writer.finish(self.row_id, result)
            except Exception as e:
                log.warning("audit end not recorded for row %s: %s", self.row_id, _short(e))
        self._close()
        return False   # never swallow the body's exception

    def lineage(self, source_object, target_object, row_count=None, target_version=None, detail=None):
        """Record source -> target lineage for this step's batch (best effort)."""
        try:
            self.writer.lineage({"pipeline_run_id": self.row["pipeline_run_id"], "task_name": self.row["task_name"],
                                 "batch_id": self.row["batch_id"] or self.row["pipeline_run_id"],
                                 "source_object": source_object, "target_object": target_object,
                                 "row_count": row_count, "target_version": target_version, "detail": detail})
        except Exception as exc:
            log.warning("lineage not recorded %s -> %s: %s", source_object, target_object, _short(exc))

    def schema_changes(self, table_name, changes):
        """Record DriftReport.changes() rows in audit.schema_changes (best effort)."""
        for change_type, column, old_type, new_type, action in changes:
            try:
                self.writer.schema_change({
                    "pipeline_run_id": self.row["pipeline_run_id"], "task_name": self.row["task_name"],
                    "batch_id": self.row["batch_id"], "table_name": table_name, "change_type": change_type,
                    "column_name": column, "old_type": old_type, "new_type": new_type, "action": action})
            except Exception as exc:
                log.warning("schema change not recorded for %s.%s: %s", table_name, column, _short(exc))

    def _close(self):
        close = getattr(self.writer, "close", None)
        if close:
            close()


def _short(exc):
    return str(exc).splitlines()[0][:300] if str(exc) else type(exc).__name__
