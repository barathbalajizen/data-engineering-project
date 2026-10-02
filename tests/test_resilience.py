import pytest

from resilience import retry


def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    @retry(attempts=3, base_delay=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("db down")
        return "ok"

    assert flaky() == "ok" and calls["n"] == 3


def test_retry_gives_up_and_raises():
    calls = {"n": 0}

    @retry(attempts=3, base_delay=0)
    def always_fails():
        calls["n"] += 1
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError):
        always_fails()
    assert calls["n"] == 3
