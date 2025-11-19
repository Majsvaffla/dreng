from __future__ import annotations

import datetime
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from dreng.constants import Priority
from dreng.decorators import task
from dreng.utils import TimeLimit
from dreng.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from dreng.constants import Delivery
    from dreng.decorators import TaskDecorator, TaskDecoratorWrapper
    from dreng.task import Task, TaskFunction

    type TaskRunnerHelper = Callable[[], None]


@pytest.fixture
def mock_task(settings: Any) -> Iterator[TaskDecorator]:
    settings.DRENG_DEFAULT_TIME_LIMITS_BY_QUEUE["tests"] = TimeLimit(max=timedelta(seconds=2), excessive_time_factor=2)

    def wrapper(
        *,
        time_limit: timedelta,
        delivery: Delivery = "at_least_once",
        repeat_at: timedelta | datetime.time | None = None,
        **real_task_kwargs: Any,
    ) -> TaskDecoratorWrapper:
        def decorator(task_function: TaskFunction) -> Task:
            if repeat_at is not None:
                settings.DRENG_REPEATING_TASKS.add(f"{task_function.__module__}.{task_function.__name__}")
            real_task = task(  # type: ignore[call-overload]
                priority=Priority.high,
                delivery=delivery,
                queue="tests",
                time_limit=time_limit,
                repeat_at=repeat_at,
                **real_task_kwargs,
            )(task_function)
            task_function.__globals__[task_function.__name__] = real_task
            return real_task  # type: ignore[no-any-return]

        return decorator

    yield wrapper

    del settings.DRENG_DEFAULT_TIME_LIMITS_BY_QUEUE["tests"]


@pytest.fixture
def some_task(mock_task: TaskDecorator) -> Task:
    @mock_task(time_limit=timedelta(seconds=2))
    def some_task() -> None:
        pass

    return some_task


@pytest.fixture
def worker() -> Worker:
    return Worker(
        {"tests"},  # type: ignore[arg-type]
    )
