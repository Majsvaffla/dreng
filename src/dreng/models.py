from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, Literal, TypedDict
from uuid import UUID, uuid4

from django.contrib.postgres.functions import TransactionNow
from django.db import connection, models
from django.utils import timezone

from .constants import STATUS_FAILURE
from .utils import JSONDecoder, JSONEncoder, import_task

__all__ = ["FailedJob", "FailedJobDict", "Job", "JobDict", "TaskState", "TaskStateDict"]

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from .queue import Queue
    from .utils import Decoded

    class JobDict(TypedDict):
        id: int
        created_at: datetime
        execute_at: datetime
        priority: int
        task: str
        args: dict[str, Decoded]
        expire_at: datetime | None
        retry_count: int
        state: TaskStateDict | None
        queue: Queue

    class TaskStateDict(TypedDict):
        identifier: UUID
        status: str

    type JobFailureReason = Literal["exception", "claim_count_limit_reached"]

    class FailedJobDict(TypedDict):
        id: int
        task: str
        args: Mapping[str, Decoded]
        failed_at: datetime
        failure_reason: JobFailureReason
        job_id: int
        state: TaskStateDict | None


logger = logging.getLogger(__name__)


KEEP_FAILED_JOBS_LIMIT: Final = timedelta(days=90)


class TaskStateManager(models.Manager["TaskState"]):
    def expired(self) -> models.QuerySet[TaskState]:
        return self.get_queryset().filter(expires_at__lte=timezone.now(), failedjob__isnull=True)

    def active(self) -> models.QuerySet[TaskState]:
        return self.filter(expires_at__gt=timezone.now())


class TaskState(models.Model):
    identifier = models.UUIDField(primary_key=True, default=uuid4)
    status = models.CharField(max_length=50)
    expires_at = models.DateTimeField()
    data = models.JSONField(null=True, encoder=JSONEncoder, decoder=JSONDecoder)

    objects = TaskStateManager()

    class Meta:
        indexes = [models.Index(fields=["expires_at"])]

    def __str__(self) -> str:
        return f"{self.identifier}: {self.status}"

    def save(
        self,
        force_insert: bool | tuple[models.base.ModelBase, ...] = False,
        force_update: bool = False,
        using: str | None = None,
        update_fields: Iterable[str] | None = None,
    ) -> None:
        self.expires_at = (
            datetime.max.replace(tzinfo=UTC) if self.status == STATUS_FAILURE else timezone.now() + timedelta(hours=1)
        )
        if force_insert:
            update_fields = None
        elif update_fields:
            update_fields = ["expires_at", *update_fields]

        return super().save(
            force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields
        )

    def as_dict(self) -> TaskStateDict:
        return {
            "identifier": self.identifier,
            "status": self.status,
        }


class JobQuerySet(models.QuerySet["Job"]):
    pass


class JobManager(models.Manager["Job"]):
    def get_queryset(self) -> JobQuerySet:
        return JobQuerySet(self.model)

    def dequeue(self, applicable_queues: Iterable[Queue]) -> Job:
        """
        Returns the first available job and raises Job.DoesNotExist if there aren't any.

        For at-most-once delivery, commit the transaction before processing the job.
        For at-least-once delivery, dequeue and finish processing the job in the same transaction.

        To put a job back in the queue, call .save(force_insert=True) on the returned object.
        """
        table_name = connection.ops.quote_name(self.model._meta.db_table)
        jobs = list[Job](
            self.raw(
                f"""
                DELETE FROM {table_name}
                WHERE id = (
                    SELECT id
                    FROM {table_name}
                    WHERE execute_at <= now()
                    AND queue IN %s
                    ORDER BY priority DESC, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *;
                """,
                [tuple(applicable_queues)],
            )
        )
        assert len(jobs) <= 1
        if not jobs:
            raise Job.DoesNotExist("There are no available jobs.")
        return jobs[0]


class BaseJob(models.Model):
    task = models.CharField(max_length=255)
    args = models.JSONField(encoder=JSONEncoder, decoder=JSONDecoder)
    state = models.OneToOneField(TaskState, on_delete=models.PROTECT, null=True)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return f"{self.pk}: {self.task}"


class Job(BaseJob):
    created_at = models.DateTimeField(default=TransactionNow)
    execute_at = models.DateTimeField(default=TransactionNow)
    priority = models.IntegerField(default=0, help_text="Jobs with higher priority will be processed first.")
    expire_at = models.DateTimeField(default=None, null=True)
    retry_count = models.PositiveSmallIntegerField(default=0)
    queue = models.CharField(max_length=50)

    objects = JobManager()

    class Meta:
        indexes = [models.Index(fields=["execute_at"])]

    def as_dict(self) -> JobDict:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "execute_at": self.execute_at,
            "priority": self.priority,
            "task": self.task,
            "args": self.args,
            "expire_at": self.expire_at,
            "retry_count": self.retry_count,
            "state": self.state.as_dict() if self.state else None,
            "queue": self.queue,  # type: ignore[typeddict-item]
        }


class FailedJobQuerySet(models.QuerySet["FailedJob"]):
    pass


class FailedJobManager(models.Manager["FailedJob"]):
    def get_queryset(self) -> FailedJobQuerySet:
        return FailedJobQuerySet(self.model)

    def delete_old(self) -> None:
        self.get_queryset().filter(failed_at__lt=timezone.now() - KEEP_FAILED_JOBS_LIMIT).delete()


class FailedJob(BaseJob):
    failed_at = models.DateTimeField()
    failure_reason = models.CharField(max_length=100)
    job_id = models.BigIntegerField(help_text="ID of the original Job instance. Stored for debugging purposes.")

    objects = FailedJobManager()

    def rerun(self) -> None:
        import_task(self.task).enqueue(**self.args)

    def as_dict(self) -> FailedJobDict:
        return {
            "id": self.pk,
            "task": self.task,
            "args": self.args,
            "failed_at": self.failed_at,
            "failure_reason": self.failure_reason,  # type: ignore[typeddict-item]
            "job_id": self.job_id,
            "state": self.state.as_dict() if self.state else None,
        }
