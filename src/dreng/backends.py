from __future__ import annotations

from typing import TYPE_CHECKING, Any

import django.tasks
from django.db import connection
from django.tasks.backends.base import BaseTaskBackend
from django.utils.module_loading import import_string

from dreng.utils import JSONDecoder, JSONEncoder

from .decorators import StatelessTask

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .utils import TimeLimit


class PostgreSQLBackend(BaseTaskBackend):
    """
    This task backend requires a database engine based on PostgreSQL.

    A Task backend is a class that inherits BaseTaskBackend. At a minimum, it must implement BaseTaskBackend.enqueue().
    (https://docs.djangoproject.com/en/6.0/topics/tasks/#third-party-backends)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.default_delivery = self.options.setdefault("default_delivery", "at_least_once")
        self.default_time_limits_by_queue: dict[str, TimeLimit] = self.options.setdefault(
            "default_time_limits_by_queue", {}
        )
        self.repeating_tasks: set[str] = self.options.setdefault("repeating_tasks", set())
        self.cache_exceptions: tuple[type[Exception]] = tuple(
            import_string(e) for e in self.options.setdefault("cache_exceptions", set())
        )
        self.database_exceptions: tuple[type[Exception]] = tuple(
            import_string(e) for e in self.options.setdefault("database_exceptions", set())
        )

    def enqueue(
        self, task: django.tasks.Task[Any, Any], args: Sequence[Any], kwargs: dict[str, Any]
    ) -> django.tasks.TaskResult[Any, Any]:
        if len(args) > 0:
            raise NotImplementedError("Positional arguments are not yet supported.")
        enqueued_task = StatelessTask.from_django_task(task).enqueue()
        job = enqueued_task.job
        return django.tasks.TaskResult(
            task=task,
            id=str(job.id),
            status=django.tasks.TaskResultStatus.READY,
            enqueued_at=job.created_at,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=[],
            kwargs=JSONDecoder().decode(JSONEncoder().encode(kwargs)),
            backend=task.backend,
            errors=[],
            worker_ids=[],
        )

    def validate_task(self, task: django.tasks.Task[Any, Any]) -> None:
        if connection.vendor != "postgresql":
            raise django.tasks.exceptions.InvalidTask("Backend only supports PostgreSQL as a database engine.")
        super().validate_task(task)
