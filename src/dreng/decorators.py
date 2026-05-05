from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends

from .constants import (
    DEFAULT_WARN_RETRIES,
    STATUS_PENDING,
    Priority,
)
from .task import EnqueuedTask, Task

__all__ = ["StatefulEnqueuedTask", "StatefulTask", "StatelessEnqueuedTask", "StatelessTask", "task"]

if TYPE_CHECKING:
    import datetime
    from collections.abc import Callable

    import django.tasks

    from .constants import Delivery
    from .task import ExceptionSequence, TaskFunction
    from .utils import Decoded

    type TaskDecoratorWrapper = Callable[[TaskFunction], Task]
    type TaskDecorator = Callable[..., TaskDecoratorWrapper]


@dataclass(frozen=True)
class StatefulEnqueuedTask(EnqueuedTask):
    state_identifier: str


@dataclass(frozen=True)
class StatelessEnqueuedTask(EnqueuedTask):
    pass


class StatefulTask(Task):
    def enqueue(
        self,
        *,
        execute_at: datetime.datetime | None = None,
        expire_at: datetime.datetime | None = None,
        **task_kwargs: Decoded,
    ) -> StatefulEnqueuedTask:
        from .models import TaskState

        state = TaskState.objects.create(status=STATUS_PENDING)
        job = self._enqueue(
            task_kwargs,
            execute_at=execute_at,
            expire_at=expire_at,
            state=state,
        )
        return StatefulEnqueuedTask(job, str(state.identifier))


class StatelessTask(Task):
    @classmethod
    def from_django_task(cls, task: django.tasks.Task[Any, Any]) -> StatelessTask:
        from .backends import PostgreSQLBackend
        from .utils import TimeLimit

        backend = task.get_backend()
        assert isinstance(backend, PostgreSQLBackend)
        return cls(
            task_function=task.func,
            retry_exceptions=(),
            priority=Priority(task.priority),
            delivery=backend.default_delivery,
            queue=task.queue_name,
            time_limit=TimeLimit.__DangerouslyDisableTimeLimit__(),
            backend=backend,
        )

    def enqueue(
        self,
        *,
        execute_at: datetime.datetime | None = None,
        expire_at: datetime.datetime | None = None,
        **task_kwargs: Decoded,
    ) -> StatelessEnqueuedTask:
        return StatelessEnqueuedTask(self._enqueue(task_kwargs, execute_at=execute_at, expire_at=expire_at))


@overload
def task(
    *,
    retry_exceptions: ExceptionSequence | None = None,
    warn_after_retries: int | None = DEFAULT_WARN_RETRIES,
    priority: Priority,
    delivery: Literal["at_least_once"],
    time_limit: datetime.timedelta,
    repeat_at: datetime.timedelta | datetime.time | None = None,
    queue: str,
    backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Callable[[TaskFunction], StatelessTask]: ...


@overload
def task(
    *,
    retry_exceptions: ExceptionSequence | None = None,
    warn_after_retries: int | None = DEFAULT_WARN_RETRIES,
    priority: Priority,
    delivery: Literal["at_most_once"],
    time_limit: datetime.timedelta,
    repeat_at: None = None,
    queue: str,
    backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Callable[[TaskFunction], StatelessTask]: ...


@overload
def task(
    *,
    keep_state: Literal[True],
    retry_exceptions: ExceptionSequence | None = None,
    warn_after_retries: int | None = DEFAULT_WARN_RETRIES,
    priority: Priority,
    delivery: Delivery,
    time_limit: datetime.timedelta,
    queue: str,
    backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Callable[[TaskFunction], StatefulTask]: ...


def task(
    *,
    retry_exceptions: ExceptionSequence | None = None,
    warn_after_retries: int | None = DEFAULT_WARN_RETRIES,
    keep_state: bool = False,
    priority: Priority,
    delivery: Delivery,
    time_limit: datetime.timedelta,
    repeat_at: datetime.timedelta | datetime.time | None = None,
    queue: str,
    backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Callable[[TaskFunction], Task]:
    from .backends import PostgreSQLBackend

    backend = task_backends[backend_alias]
    assert isinstance(backend, PostgreSQLBackend)

    def decorator(f: TaskFunction) -> Task:
        if keep_state:
            assert repeat_at is None
            return StatefulTask(
                task_function=f,
                retry_exceptions=retry_exceptions or [],
                warn_after_retries=warn_after_retries,
                priority=priority,
                time_limit=time_limit,
                delivery=delivery,
                queue=queue,
                backend=backend,
            )
        else:
            return StatelessTask(
                task_function=f,
                retry_exceptions=retry_exceptions or (),
                warn_after_retries=warn_after_retries,
                priority=priority,
                time_limit=time_limit,
                delivery=delivery,
                repeat_at=repeat_at,
                queue=queue,
                backend=backend,
            )

    return decorator
