"""Small retry helper with exponential backoff (pure Python, unit-testable)."""
import functools
import logging
import time


def retry(attempts=3, base_delay=2.0, exceptions=(Exception,)):
    """Retry a function on failure: waits base_delay, 2x, 4x ... between attempts."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    logging.getLogger("retry").warning(
                        "%s failed (%s). Retry %d/%d in %.0fs", fn.__name__, exc, attempt, attempts - 1, delay)
                    time.sleep(delay)
        return wrapper
    return decorator
