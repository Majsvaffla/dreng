from __future__ import annotations

import datetime
import random
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import models, transaction
from django.utils import timezone

from . import logging
from .exceptions import SilentFail
from .signals import before_task_run

__all__ = ["EnqueuedTask", "ExceptionSequence", "Task", "TaskFunction"]

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from .constants import Delivery, Priority
    from .models import Job, TaskState
    from .queue import Queue
    from .utils import Decoded

    type TaskFunction = Callable[..., Any]
    type ExceptionSequence = Sequence[type[Exception]]
    type _CalledStatus = Literal["succeeded", "retried", "exception", "expired", "silent_fail"]

    class _CalledResult(TypedDict):
        value: NotRequired[Any]
        exception: NotRequired[BaseException]
        state: NotRequired[TaskState]
        status: _CalledStatus

    JobArgs = Mapping[str, Decoded]
    JobArgsSequence = (
        Sequence[JobArgs]
        # Return type of models.QuerySet.values is TypedDict[...] thus we can't use the JobArgs type here.
        | models.QuerySet[models.Model, Mapping[str, Any]]
    )

logger = logging.getLogger(__name__)


def _retry_at(retry_count: int) -> datetime.datetime:
    jitter = random.randrange(0, 10)
    max_exponent = min(retry_count, 10)  # Cap retry backoff to avoid overflowing.
    return timezone.now() + min(
        timedelta(minutes=10),
        timedelta(seconds=2**max_exponent + jitter),
    )


@dataclass(frozen=True)
class EnqueuedTask:
    job: Job


class Task:
    def __init__(
        self,
        task_function: TaskFunction,
        retry_exceptions: ExceptionSequence,
        priority: Priority,
        delivery: Delivery,
        queue: Queue,
        time_limit: timedelta,
        warn_after_retries: int | None = None,
        repeat_at: timedelta | datetime.time | None = None,
    ) -> None:
        self._task_function = task_function
        self._retry_exceptions = tuple(retry_exceptions)
        self._warn_after_retries = warn_after_retries
        self._priority = priority
        self.time_limit = time_limit
        self.delivery = delivery
        self._repeat_at = repeat_at
        self.queue = queue
        self.import_path = f"{self._task_function.__module__}.{self._task_function.__name__}"

        if repeat_at is not None:
            if delivery == "at_most_once":
                raise ValueError(f"Repeating task {self.import_path} can't use at-most-once-delivery.")
            if self.import_path not in settings.DRENG_REPEATING_TASKS:
                raise ImproperlyConfigured(f"Task {self.import_path} is not in settings.DRENG_REPEATING_TASKS.")

    def _run(self, job: Job) -> Any:
        with ExitStack() as stack:
            if self.delivery == "at_least_once":
                stack.enter_context(transaction.atomic(durable=False))
            stack.enter_context(
                settings.DRENG_DEFAULT_TIME_LIMITS_BY_QUEUE[self.queue].as_context_manager(self, job, self.time_limit)
            )
            before_task_run.send("dreng.task", job=job, task=self, stack=stack)
            return self._task_function(**job.args)

    def __inner_call__(self, job: Job) -> _CalledResult:
        if job.expire_at and job.expire_at < timezone.now():
            logger.info(
                f"Job {job} has expired.",
                extra={
                    "job": job.as_dict(),
                },
            )
            return {"status": "expired"}
        try:
            result = self._run(job)
            if job.state:
                return {"state": job.state, "value": result, "status": "succeeded"}
            return {"value": result, "status": "succeeded"}

        except self._retry_exceptions as e:
            new_retry_count = job.retry_count + 1
            new_job = self._enqueue(
                execute_at=_retry_at(new_retry_count),
                retry_count=new_retry_count,
                expire_at=job.expire_at,
                state=job.state,
                args=job.args,
            )
            logger.log(
                logging.WARNING if self._warn_after_retries == new_retry_count else logging.INFO,
                f"Job {job} got retry exception. A new job will retry.",
                extra={
                    "old_job": job.as_dict(),
                    "new_job": new_job.as_dict(),
                    "exception_type": type(e),
                    "exception_reason": str(e),
                },
            )
            return {"status": "retried"}
        except SilentFail as f:
            if job.state:
                return {"state": job.state, "exception": f, "status": "silent_fail"}
            return {"exception": f, "status": "silent_fail"}
        except Exception as e:
            if job.state:
                return {"state": job.state, "exception": e, "status": "exception"}
            return {"exception": e, "status": "exception"}

    @property
    def is_repeating(self) -> bool:
        return self._repeat_at is not None

    @property
    def next_execute_at(self) -> datetime.datetime | None:
        # self._repeat_at is either timedelta, time or None
        if self._repeat_at is None:
            return None
        local_now = timezone.localtime()
        if isinstance(self._repeat_at, timedelta):
            return local_now + self._repeat_at
        execute_at = datetime.datetime.combine(
            date=local_now.date(), time=self._repeat_at, tzinfo=timezone.get_current_timezone()
        )
        if execute_at <= local_now:
            return execute_at + timedelta(days=1)
        return execute_at

    def __call__(self, job: Job) -> _CalledResult:
        base_task_returns = self.__inner_call__(job)
        may_be_repeated = base_task_returns["status"] not in ("expired", "retried")
        if may_be_repeated and (next_execute_at := self.next_execute_at):
            assert next_execute_at > job.execute_at
            self.enqueue(execute_at=next_execute_at)

        return base_task_returns

    def _create_job(
        self,
        args: JobArgs,
        *,
        execute_at: datetime.datetime | None = None,
        expire_at: datetime.datetime | None = None,
        state: TaskState | None = None,
        retry_count: int = 0,
    ) -> Job:
        from .models import Job

        job = Job(
            task=self.import_path,
            args=args,
            priority=self._priority.value,
            expire_at=expire_at,
            retry_count=retry_count,
            state=state,
            queue=self.queue,
        )
        if execute_at is not None:
            job.execute_at = execute_at
        return job

    def _enqueue(
        self,
        args: JobArgs,
        *,
        execute_at: datetime.datetime | None = None,
        expire_at: datetime.datetime | None = None,
        state: TaskState | None = None,
        retry_count: int = 0,
    ) -> Job:
        job = self._create_job(args, execute_at=execute_at, expire_at=expire_at, state=state, retry_count=retry_count)
        job.save(using="default")
        return job

    def enqueue(
        self,
        *,
        execute_at: datetime.datetime | None = None,
        expire_at: datetime.datetime | None = None,
        **job_args: Decoded,
    ) -> EnqueuedTask:
        raise NotImplementedError

    def enqueue_and_execute_evenly(
        self,
        job_args: JobArgsSequence,
        span: timedelta,
    ) -> None:
        if len(job_args) == 0:
            return
        from .models import Job

        interval = span / len(job_args)
        now_ = timezone.now()
        Job.objects.bulk_create(
            self._create_job(args, execute_at=now_ + index * interval, expire_at=None)
            for index, args in enumerate(job_args)
        )

    def bulk_enqueue(self, job_args: Iterable[JobArgs] | JobArgsSequence) -> list[Job]:
        from .models import Job

        return Job.objects.bulk_create(self._create_job(args) for args in job_args)
