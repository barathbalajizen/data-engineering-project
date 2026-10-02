"""Decides which slice of the source an extract run should read (pure Python, unit-testable)."""
from datetime import datetime, timedelta

EPOCH = datetime(1900, 1, 1)


def _parse(value):
    return datetime.fromisoformat(value.strip())


def extraction_window(watermark, lookback_minutes=10, start=None, end=None):
    """Return (lower_inclusive, upper_exclusive_or_None, is_backfill).

    Normal run : lower = watermark - lookback (overlap window catches late-committed rows).
    Backfill   : explicit [start, end) on updated_at; the watermark must NOT be touched.
    """
    if lookback_minutes < 0:
        raise ValueError("lookback_minutes must be >= 0")
    if start or end:
        lower = _parse(start) if start else EPOCH
        upper = _parse(end) if end else None
        if upper is not None and upper <= lower:
            raise ValueError(f"end ({upper}) must be after start ({lower})")
        return lower, upper, True
    return watermark - timedelta(minutes=lookback_minutes), None, False


def split_window(start, end, chunk_days=31, max_chunks=1000):
    """Split [start, end) into consecutive chunks of at most chunk_days.

    Returns a list of (start, end) strings that extract_bronze.py --start/--end accepts.
    Each chunk becomes its own Prefect task, so one failed chunk retries without redoing the others.
    """
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    lo, hi = _parse(start), _parse(end)
    if hi <= lo:
        raise ValueError(f"end ({hi}) must be after start ({lo})")
    out, cur = [], lo
    while cur < hi:
        nxt = min(cur + timedelta(days=chunk_days), hi)
        out.append((cur.isoformat(sep=" "), nxt.isoformat(sep=" ")))
        cur = nxt
        if len(out) > max_chunks:
            raise ValueError(f"window needs more than {max_chunks} chunks; raise chunk_days")
    return out
