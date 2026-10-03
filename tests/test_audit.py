"""Unit tests for the audit helpers and the migration ordering (no Postgres or Spark needed)."""
import re
from datetime import datetime, timezone

import pytest

from audit import MAX_ERROR_CHARS, audit_step, counts_since, make_batch_id, merge_counts
from migrate import pending_migrations


class FakeWriter:
    def __init__(self, fail_start=False, fail_finish=False):
        self.started, self.finished, self.lineages = [], [], []
        self.fail_start, self.fail_finish, self.closed = fail_start, fail_finish, False

    def start(self, row):
        if self.fail_start:
            raise ConnectionError("db down")
        self.started.append(row)
        return 42

    def finish(self, row_id, row):
        if self.fail_finish:
            raise ConnectionError("db down")
        self.finished.append((row_id, row))

    def lineage(self, row):
        self.lineages.append(row)

    def close(self):
        self.closed = True


# ----------------------------------------------------------------- batch ids
def test_batch_id_is_readable_and_unique():
    now = datetime(2026, 10, 3, 10, 15, 0, tzinfo=timezone.utc)
    a, b = make_batch_id("orders", now), make_batch_id("orders", now)
    assert re.fullmatch(r"orders-20261003T101500-[0-9a-f]{8}", a)
    assert a != b


# ------------------------------------------------------------- delta metrics
def test_merge_counts_from_merge_metrics():
    m = {"numTargetRowsInserted": "5", "numTargetRowsUpdated": "3", "numTargetRowsDeleted": "0",
         "numOutputRows": "8"}
    assert merge_counts(m) == (5, 3, 0)


def test_merge_counts_from_plain_write():
    assert merge_counts({"numOutputRows": "20000", "numFiles": "1"}) == (20000, 0, 0)


def test_merge_counts_handles_missing_and_bad_values():
    assert merge_counts(None) == (0, 0, 0)
    assert merge_counts({"numTargetRowsInserted": "7", "numTargetRowsUpdated": None}) == (7, 0, 0)


def test_no_new_delta_version_means_nothing_written():
    # Delta skips the commit for a no-op MERGE: the latest commit is an OLDER write and must not be counted
    old_write = {"numOutputRows": "20000"}
    assert counts_since(version_before=4, version_after=4, metrics=old_write) == (0, 0, 0)


def test_new_delta_version_uses_its_metrics():
    assert counts_since(4, 5, {"numTargetRowsInserted": "3", "numTargetRowsUpdated": "1"}) == (3, 1, 0)
    assert counts_since(None, 0, {"numOutputRows": "10"}) == (10, 0, 0)   # first write creates the table


# --------------------------------------------------------------- audit_step
def test_success_records_counts_and_retry_count():
    w = FakeWriter()
    with audit_step("extract-bronze", table_name="bronze.orders", batch_id="b1", run_id="r1",
                    attempt=3, writer=w) as a:
        a.source_rows, a.inserted, a.updated, a.rejected = 10, 7, 2, 1
    start = w.started[0]
    assert (start["pipeline_run_id"], start["task_name"], start["level"]) == ("r1", "extract-bronze", "table")
    assert start["retry_count"] == 2                      # attempt 3 = 2 retries
    row_id, end = w.finished[0]
    assert row_id == 42 and end["status"] == "SUCCESS" and end["error"] is None
    assert (end["source_rows"], end["inserted"], end["updated"], end["rejected"]) == (10, 7, 2, 1)
    assert end["duration"] >= 0 and w.closed


def test_failure_is_recorded_and_reraised():
    w = FakeWriter()
    with pytest.raises(ValueError, match="boom"):
        with audit_step("transform-silver", run_id="r1", writer=w):
            raise ValueError("boom")
    end = w.finished[0][1]
    assert end["status"] == "FAILED" and end["error"] == "ValueError: boom"


def test_long_errors_are_truncated():
    w = FakeWriter()
    with pytest.raises(RuntimeError):
        with audit_step("t", run_id="r", writer=w):
            raise RuntimeError("x" * 10_000)
    assert len(w.finished[0][1]["error"]) == MAX_ERROR_CHARS


def test_audit_outage_never_breaks_the_work():
    w = FakeWriter(fail_start=True)
    ran = []
    with audit_step("t", run_id="r", writer=w):
        ran.append(True)
    assert ran == [True] and w.finished == []           # body ran; nothing to close


def test_audit_finish_failure_is_swallowed_but_body_error_is_not():
    with audit_step("t", run_id="r", writer=FakeWriter(fail_finish=True)):
        pass                                             # no exception from the audit itself
    with pytest.raises(KeyError):
        with audit_step("t", run_id="r", writer=FakeWriter(fail_finish=True)):
            raise KeyError("real error")


def test_context_comes_from_environment(monkeypatch):
    monkeypatch.setenv("RUN_ID", "run-env")
    monkeypatch.setenv("TASK_NAME", "load-warehouse")
    monkeypatch.setenv("ATTEMPT", "2")
    w = FakeWriter()
    with audit_step(writer=w):
        pass
    row = w.started[0]
    assert (row["pipeline_run_id"], row["task_name"], row["retry_count"]) == ("run-env", "load-warehouse", 1)


def test_lineage_uses_step_batch_and_run():
    w = FakeWriter()
    with audit_step("extract-bronze", batch_id="b9", run_id="r1", writer=w) as a:
        a.lineage("postgres:source.orders", "delta:bronze/orders", 5, 3, "window")
    assert w.lineages[0] == {"pipeline_run_id": "r1", "task_name": "extract-bronze", "batch_id": "b9",
                             "source_object": "postgres:source.orders", "target_object": "delta:bronze/orders",
                             "row_count": 5, "target_version": 3, "detail": "window"}


# ----------------------------------------------------------------- migrations
def test_pending_migrations_skips_applied_and_sorts():
    files = ["/m/002_b.sql", "/m/001_a.sql", "/m/010_c.sql"]
    assert pending_migrations(files, {"001_a"}) == ["/m/002_b.sql", "/m/010_c.sql"]
    assert pending_migrations(files, {"001_a", "002_b", "010_c"}) == []
