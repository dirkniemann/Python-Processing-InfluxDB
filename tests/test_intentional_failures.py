"""Intentional failing tests to illustrate failure output (English messages).

Marked as xfail so the suite stays green while still demonstrating clear failure text
when run with `pytest -rx` to view xfail reasons.
"""

import pytest


@pytest.mark.xfail(reason="Intentional failure: key 'missing' not found in data", strict=False)
def test_intentional_failure_missing_key():
    data = {"expected": True}
    assert "missing" in data, "Intentional failure: key 'missing' not found in data"


@pytest.mark.xfail(reason="Intentional failure: expected value 42 but got 41", strict=False)
def test_intentional_failure_wrong_value():
    value = 41
    assert value == 42, "Intentional failure: expected value 42 but got 41"
