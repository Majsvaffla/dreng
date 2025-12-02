from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

import pytest
import time_machine

from dreng.models import Job
from tests.helpers import datetime_utc

if TYPE_CHECKING:
    from dreng.decorators import TaskDecorator
    from dreng.task import Task


@pytest.mark.django_db
def test_bulk_enqueue(some_task: Task) -> None:
    some_task.bulk_enqueue({"i": i} for i in range(4))
    assert {j.execute_at for j in Job.objects.all()} == {j.execute_at if (j := Job.objects.first()) else None}
    assert {j.args["i"] for j in Job.objects.all()} == {0, 1, 2, 3}


@pytest.mark.parametrize(
    ("time_zone", "repeat_at", "travel_to", "expected_next_execute_at"),
    [
        (
            "UTC",
            datetime.timedelta(hours=2),
            datetime_utc(2025, 12, 2, 1),
            datetime_utc(2025, 12, 2, 3),
        ),
        (
            "Europe/Stockholm",
            datetime.timedelta(hours=2),
            datetime_utc(2025, 12, 31, 22),
            datetime_utc(2026, 1, 1, 0),
        ),
        (
            "UTC",
            datetime.time(3, 4, 5, 6),
            datetime_utc(2026, 1, 2, 3, 4),
            datetime_utc(2026, 1, 2, 3, 4, 5, 6),
        ),
        (
            "UTC",
            datetime.time(0, 30),
            datetime_utc(2025, 12, 1, 0, 30),
            datetime_utc(2025, 12, 2, 0, 30),
        ),
        (
            "Europe/Stockholm",
            datetime.time(0, 30),
            datetime_utc(2025, 11, 30, 23, 30),
            datetime_utc(2025, 12, 1, 23, 30),
        ),
    ],
)
def test_next_execute_at(
    mock_task: TaskDecorator,
    settings: Any,
    time_zone: str,
    repeat_at: datetime.time | datetime.timedelta,
    travel_to: datetime.datetime,
    expected_next_execute_at: datetime.datetime,
) -> None:
    settings.TIME_ZONE = time_zone

    @mock_task(
        repeat_at=repeat_at,
        time_limit=datetime.timedelta(minutes=1),
    )
    def repeating_task() -> None:
        pass

    with time_machine.travel(travel_to):
        assert repeating_task.next_execute_at == expected_next_execute_at
