import pytest

from windowing import split_window


def test_splits_into_consecutive_chunks_without_gaps():
    chunks = split_window("2024-01-01", "2024-03-01", chunk_days=31)
    assert chunks == [("2024-01-01 00:00:00", "2024-02-01 00:00:00"),
                      ("2024-02-01 00:00:00", "2024-03-01 00:00:00")]


def test_last_chunk_is_clipped_to_end():
    chunks = split_window("2024-01-01", "2024-01-10", chunk_days=7)
    assert chunks[-1] == ("2024-01-08 00:00:00", "2024-01-10 00:00:00")
    assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))


def test_single_chunk_when_window_is_small():
    assert len(split_window("2024-01-01", "2024-01-02", chunk_days=31)) == 1


def test_bad_inputs_rejected():
    with pytest.raises(ValueError):
        split_window("2024-02-01", "2024-01-01")
    with pytest.raises(ValueError):
        split_window("2024-01-01", "2024-02-01", chunk_days=0)
    with pytest.raises(ValueError):
        split_window("2000-01-01", "2024-01-01", chunk_days=1, max_chunks=100)
