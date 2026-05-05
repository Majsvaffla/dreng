from __future__ import annotations

from typing import TYPE_CHECKING, Any

import django.tasks
from django.db import connection
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils.module_loading import import_string

from dreng.utils import JSONDecoder, JSONEncoder

from .decorators import StatefulTask
from .utils import import_task

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .utils import TimeLimit


class DrengBackend(BaseTaskBackend):
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
        enqueued_task = StatefulTask.from_django_task(task).enqueue(*args, **kwargs)
        job = enqueued_task.job
        assert job.state is not None
        return django.tasks.TaskResult(
            task=task,
            id=str(job.id),
            status=django.tasks.TaskResultStatus.READY,
            enqueued_at=job.created_at,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=JSONDecoder().decode(JSONEncoder().encode(args)),
            kwargs=JSONDecoder().decode(JSONEncoder().encode(kwargs)),
            backend=task.backend,
            errors=[],
            worker_ids=[],
        )

    def get_result(self, result_id: str) -> django.tasks.TaskResult[Any, Any]:
        from .models import FailedJob, Job

        try:
            job = Job.objects.get(pk=result_id)
        except Job.DoesNotExist:
            try:
                failed_job = FailedJob.objects.get(job_id=result_id)
            except FailedJob.DoesNotExist:
                status = django.tasks.TaskResultStatus.SUCCESSFUL
            else:
                status = django.tasks.TaskResultStatus.FAILED
        else:
            status = django.tasks.TaskResultStatus.READY

        task = django.tasks.Task(
            priority=job.priority,
            backend="hejsan",
            queue_name=job.queue,
            run_after=job.execute_at,
            func=import_task(job.task),
        )
        return django.tasks.TaskResult(
            task=task,
            id=str(result_id),
            status=status,
            enqueued_at=job.created_at,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=job.args,
            kwargs=job.kwargs,
            backend=task.backend,
            errors=[],
            worker_ids=[],
        )

    def validate_task(self, task: django.tasks.Task[Any, Any]) -> None:
        if connection.vendor != "postgresql":
            raise django.tasks.exceptions.InvalidTask("Backend only supports PostgreSQL as a database engine.")
        super().validate_task(task)
