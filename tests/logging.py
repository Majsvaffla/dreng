from typing import Any

import pytest

from dreng import logging


@pytest.mark.parametrize(
    ("extra", "expected_job"),
    [
        (
            {"slack": {"chill": True}, "job": {"some_number": 123, "args": [1, 2, 3]}},
            {"some_number": 123, "args": "[1, 2, 3]"},
        ),
        (
            {"slack": {"chill": True}},
            {},
        ),
    ],
)
def test_JobArgsFilter(caplog: pytest.LogCaptureFixture, extra: dict[str, Any], expected_job: dict[str, Any]) -> None:
    logger = logging.getLogger("tests")
    logger.info("Working hard or hardly working?", extra=extra)
    (record,) = caplog.records
    assert hasattr(record, "slack")
    assert record.slack["chill"] is True
    assert getattr(record, "job", {}) == expected_job
