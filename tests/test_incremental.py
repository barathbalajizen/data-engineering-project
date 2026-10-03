"""Unit tests for the incremental Silver mode decision and the version classification/reconciliation."""
import pytest

from incremental import FULL, INCREMENTAL, SKIP, MemoryCheckpointStore, decide_mode
from transforms import classify_versions, reconcile_versions


@pytest.mark.parametrize("args, mode", [
    # checkpoint, bronze_version, cdf_since, target_exists, force_full
    ((5, 8, 1, True, True), FULL),          # forced
    ((5, 8, 1, False, False), FULL),        # silver missing
    ((None, 8, 1, True, False), FULL),      # first run
    ((9, 8, 1, True, False), FULL),         # bronze recreated / restored to an older version
    ((8, 8, 1, True, False), SKIP),         # nothing new
    ((8, 8, None, True, False), SKIP),      # nothing new, even without CDF
    ((5, 8, None, True, False), FULL),      # CDF off
    ((0, 8, 2, True, False), FULL),         # versions 1 has no change data
    ((1, 8, 2, True, False), INCREMENTAL),  # reads 2..8
    ((5, 8, 1, True, False), INCREMENTAL),
])
def test_decide_mode(args, mode):
    assert decide_mode(*args)[0] == mode


def test_incremental_reason_names_the_versions():
    assert decide_mode(5, 8, 1, True)[1] == "bronze versions 6..8"


def test_memory_store():
    store = MemoryCheckpointStore()
    assert store.get("orders") is None
    store.set("orders", 7, INCREMENTAL)
    assert store.get("orders") == 7


def _versions(spark, rows):
    return spark.createDataFrame(rows, "k string, v int")


def test_classify_versions(spark):
    target = _versions(spark, [("a", 2), ("b", 2), ("c", 2)])
    batch = _versions(spark, [("a", 3), ("b", 2), ("c", 1), ("d", 1)])
    assert classify_versions(batch, target, ["k"], "v") == {"new": 1, "newer": 1, "same": 1, "late": 1}
    assert classify_versions(batch, None, ["k"], "v") == {"new": 4, "newer": 0, "same": 0, "late": 0}


def test_reconcile_versions(spark):
    source = _versions(spark, [("a", 1), ("b", 2), ("c", 3), ("d", 1), ("e", 1), ("f", 1)])
    silver = _versions(spark, [("a", 1), ("b", 1), ("c", 2), ("f", 2)])
    quarantine = _versions(spark, [("b", 2), ("d", 1)])
    r = reconcile_versions(source, silver, quarantine, "k", "v")
    assert r == {"source": 6, "in_silver": 4, "quarantined_only": 1, "missing": 1,   # d quarantined, e lost
                 "stale_explained": 1, "stale_unexplained": 1, "ahead": 1}          # b explained, c not, f ahead


def test_reconcile_without_quarantine(spark):
    r = reconcile_versions(_versions(spark, [("a", 1), ("b", 1)]), _versions(spark, [("a", 1)]), None, "k", "v")
    assert (r["missing"], r["quarantined_only"], r["stale_explained"]) == (1, 0, 0)
