from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

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

    from .constants import Delivery
    from .queue import Queue
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
    queue: Queue,
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
    queue: Queue,
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
    queue: Queue,
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
    queue: Queue,
) -> Callable[[TaskFunction], Task]:
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
            )

    return decorator
