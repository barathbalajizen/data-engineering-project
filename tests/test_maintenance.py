"""Unit tests for the Delta maintenance decision and output file sizing."""
import pytest

pytest.importorskip("sqlalchemy")   # maintenance imports common (needs sqlalchemy; the CI unit job has only pyspark)

from common import sized_for_write  # noqa: E402
from maintenance import needs_compaction  # noqa: E402

MB = 1024 * 1024


@pytest.mark.parametrize("files, size, expected", [
    (100, 50 * MB, True),        # many tiny files
    (16, 16 * MB, True),         # at the threshold
    (15, 1 * MB, False),         # too few files to bother
    (20, 20 * 64 * MB, False),   # many files but already large
    (1, 1, False),
    (0, 0, False),
])
def test_needs_compaction(files, size, expected):
    assert needs_compaction(files, size, min_files=16, small_file_mb=32) is expected


def test_sized_for_write_targets_rows_per_file(spark, monkeypatch):
    import common
    df = spark.range(100).repartition(8)
    assert sized_for_write(df, 100).rdd.getNumPartitions() == 1          # small table: one file
    monkeypatch.setattr(common, "ROWS_PER_FILE", 30)
    assert common.sized_for_write(df, 100).rdd.getNumPartitions() == 4   # ceil(100 / 30)


@pytest.mark.parametrize("rows, expected", [(0, 1), (200, 1), (49_999, 1), (100_000, 2), (200_000, 4), (10**7, 4)])
def test_jdbc_write_partitions(rows, expected):
    from common import jdbc_write_partitions
    assert jdbc_write_partitions(rows) == expected
