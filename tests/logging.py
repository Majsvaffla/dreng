from typing import Any

import pytest
from django.contrib.postgres.functions import TransactionNow

from dreng import logging
from tests.helpers import datetime_utc


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
        (
            {
                "slack": {"chill": True},
                "job": {
                    "created_at": datetime_utc(2025, 12, 25, 13, 37),
                    "execute_at": datetime_utc(2025, 12, 24, 14, 47),
                },
            },
            {"created_at": datetime_utc(2025, 12, 25, 13, 37), "execute_at": datetime_utc(2025, 12, 24, 14, 47)},
        ),
        (
            {
                "slack": {"chill": True},
                "job": {
                    "created_at": TransactionNow(),
                    "execute_at": TransactionNow(),
                },
            },
            {"created_at": None, "execute_at": None},
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
