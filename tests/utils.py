from __future__ import annotations

import datetime
import json
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest

from dreng.utils import JSONDecoder, JSONEncoder, TimeLimit

if TYPE_CHECKING:
    from dreng.task import Task


def test_JSONEncoder() -> None:
    assert json.loads(
        JSONEncoder().encode(
            {
                "str": "abcd",
                "int": 123,
                "float": 45.6,
                "decimal.Decimal": Decimal("78.9"),
                "True": True,
                "False": False,
                "datetime.date": datetime.date(2023, 10, 3),
                "datetime.datetime": datetime.datetime(2023, 10, 3, 13, 37, tzinfo=ZoneInfo("Europe/Stockholm")),
                "list": [1, 2, 3],
                "tuple": (4, 5, 6),
                "None": None,
            }
        )
    ) == {
        "str": "abcd",
        "int": 123,
        "float": 45.6,
        "decimal.Decimal": {
            "__type__": "Decimal",
            "__value__": "78.9",
        },
        "True": True,
        "False": False,
        "datetime.date": {
            "__type__": "date",
            "__value__": "2023-10-03",
        },
        "datetime.datetime": {
            "__type__": "datetime",
            "__value__": "2023-10-03T13:37:00+02:00",
        },
        "list": [1, 2, 3],
        "tuple": [4, 5, 6],
        "None": None,
    }


def test_JSONDecoder() -> None:
    assert JSONDecoder().decode(
        json.dumps(
            {
                "str": "abcd",
                "int": 123,
                "float": 45.6,
                "decimal.Decimal": {
                    "__type__": "Decimal",
                    "__value__": "78.9",
                },
                "True": True,
                "False": False,
                "datetime.date": {
                    "__type__": "date",
                    "__value__": "2023-10-03",
                },
                "datetime.datetime": {
                    "__type__": "datetime",
                    "__value__": "2023-10-03T13:37:00+02:00",
                },
                "list": [1, 2, 3],
                "tuple": [4, 5, 6],
                "None": None,
            }
        )
    ) == {
        "str": "abcd",
        "int": 123,
        "float": 45.6,
        "decimal.Decimal": Decimal("78.9"),
        "True": True,
        "False": False,
        "datetime.date": datetime.date(2023, 10, 3),
        "datetime.datetime": datetime.datetime(2023, 10, 3, 13, 37, tzinfo=ZoneInfo("Europe/Stockholm")),
        "list": [1, 2, 3],
        "tuple": [4, 5, 6],
        "None": None,
    }


class Test_TimeLimit:
    def test_too_small_excessive_time_factor(self) -> None:
        with pytest.raises(ValueError, match=r"excessive_time_factor must be greater than or equal to 1\."):
            TimeLimit(max=timedelta(minutes=123), excessive_time_factor=0)

    @pytest.mark.django_db
    def test_too_big_time_limit(self, some_task: Task) -> None:
        time_limit = TimeLimit(max=timedelta(minutes=123), excessive_time_factor=1)
        some_job = some_task.enqueue().job
        with (
            pytest.raises(ValueError, match=r"7:36:00 is greater than the maximum time limit \(2:03:00\)\."),
            time_limit.as_context_manager(some_task, some_job, limit=timedelta(minutes=456)),
        ):
            pass

    @pytest.mark.django_db
    def test_DangerouslyExceedMaximumTimeLimit(self, some_task: Task) -> None:
        time_limit = TimeLimit(max=timedelta(minutes=123), excessive_time_factor=1)
        some_job = some_task.enqueue().job
        with (
            pytest.raises(AssertionError, match="Maximum time limit was dangerously exceeded"),
            time_limit.as_context_manager(
                some_task, some_job, limit=TimeLimit.__DangerouslyExceedMaximumTimeLimit__(minutes=456)
            ),
        ):
            raise AssertionError(
                "Maximum time limit was dangerously exceeded"
            )  # Raise something to exit the time limit context.
