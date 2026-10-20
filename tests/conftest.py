from __future__ import annotations

import datetime
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, DEFAULT_TASK_QUEUE_NAME, task_backends

from dreng.backends import PostgreSQLBackend
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
def mock_task(backend: PostgreSQLBackend) -> Iterator[TaskDecorator]:
    backend.default_time_limits_by_queue[DEFAULT_TASK_QUEUE_NAME] = TimeLimit(
        max=timedelta(seconds=2), excessive_time_factor=2
    )

    def wrapper(
        *,
        time_limit: timedelta,
        delivery: Delivery = "at_least_once",
        repeat_at: timedelta | datetime.time | None = None,
        **real_task_kwargs: Any,
    ) -> TaskDecoratorWrapper:
        def decorator(task_function: TaskFunction) -> Task:
            if repeat_at is not None:
                backend.repeating_tasks.add(f"{task_function.__module__}.{task_function.__name__}")
            real_task = task(
                priority=Priority.high,
                delivery=delivery,  # type: ignore[arg-type]
                queue=DEFAULT_TASK_QUEUE_NAME,
                time_limit=time_limit,
                repeat_at=repeat_at,
                **real_task_kwargs,
            )(task_function)
            task_function.__globals__[task_function.__name__] = real_task
            return real_task

        return decorator

    yield wrapper

    del backend.default_time_limits_by_queue[DEFAULT_TASK_QUEUE_NAME]


@pytest.fixture
def some_task(mock_task: TaskDecorator) -> Task:
    @mock_task(time_limit=timedelta(seconds=2))
    def some_task() -> None:
        pass

    return some_task


@pytest.fixture
def backend() -> PostgreSQLBackend:
    default_task_backend = task_backends[DEFAULT_TASK_BACKEND_ALIAS]
    assert isinstance(default_task_backend, PostgreSQLBackend)
    return default_task_backend


@pytest.fixture
def worker(backend: PostgreSQLBackend) -> Worker:
    return Worker(
        {DEFAULT_TASK_QUEUE_NAME},
        backend,
    )
