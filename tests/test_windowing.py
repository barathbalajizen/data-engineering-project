from datetime import datetime

import pytest

from windowing import extraction_window


def test_normal_run_applies_overlap():
    lo, hi, backfill = extraction_window(datetime(2024, 1, 1, 12, 0), lookback_minutes=10)
    assert lo == datetime(2024, 1, 1, 11, 50) and hi is None and backfill is False


def test_first_run_from_epoch_is_valid():
    lo, _, _ = extraction_window(datetime(1900, 1, 1), lookback_minutes=10)
    assert lo < datetime(1900, 1, 1)


def test_backfill_window_ignores_watermark():
    lo, hi, backfill = extraction_window(datetime(2024, 6, 1), 10, start="2024-01-01", end="2024-02-01")
    assert (lo, hi, backfill) == (datetime(2024, 1, 1), datetime(2024, 2, 1), True)


def test_backfill_open_ended_end():
    lo, hi, backfill = extraction_window(datetime(2024, 6, 1), 10, start="2024-01-01")
    assert lo == datetime(2024, 1, 1) and hi is None and backfill is True


def test_end_before_start_rejected():
    with pytest.raises(ValueError):
        extraction_window(datetime(2024, 6, 1), 10, start="2024-02-01", end="2024-01-01")
