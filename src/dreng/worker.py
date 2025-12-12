# cspell: words noreload
from __future__ import annotations

import signal
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, assert_never

from django.conf import settings
from django.core.cache import cache
from django.db import close_old_connections, transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from . import logging
from .constants import CLAIM_COUNT_LIMIT, STATUS_FAILURE, STATUS_SUCCESS
from .models import FailedJob, Job
from .signals import after_job_finished, after_task_imported
from .task import Task
from .utils import get_claim_count_cache_key, import_task

__all__ = ["Worker"]

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import FrameType

    from .constants import TaskStateStatus
    from .models import JobFailureReason
    from .queue import Queue
    from .task import Task
    from .utils import Decoded

logger = logging.getLogger(__name__)

type DequeueResult = Literal[
    "did_dequeue_job",
    "no_job_to_dequeue",
    "claim_count_limit_reached",
    "unable_to_import_task",
    "unable_to_check_claim_count",
]

CACHE_EXCEPTIONS: tuple[type[Exception], ...] = tuple(
    import_string(e) for e in getattr(settings, "DRENG_CACHE_EXCEPTIONS", [])
)
DATABASE_EXCEPTIONS: tuple[type[Exception], ...] = tuple(
    import_string(e) for e in getattr(settings, "DRENG_DATABASE_EXCEPTIONS", [])
)


def _log_job_exception(job: Job, exception: BaseException) -> None:
    assert exception.__traceback__ is not None
    logger.exception(
        str(exception),
        exc_info=(exception.__class__, exception, exception.__traceback__),
        extra={"job": job.as_dict()},
    )


def _log_silent_failure(job: Job) -> None:
    logger.info(f"Job {job} failed silently.", extra={"job": job.as_dict()})


def _log_claim_count_limit_reached(job: Job, claim_count: int) -> None:
    logger.warning(f"Job {job} hit claim count limit.", extra={"job": job.as_dict(), "claim_count": claim_count})


def _set_failed(job: Job, reason: JobFailureReason) -> None:
    FailedJob.objects.create(
        task=job.task,
        args=job.args,
        state=job.state,
        failed_at=timezone.now(),
        failure_reason=reason,
        job_id=job.id,
    )


def _update_state(job: Job, status: TaskStateStatus, data: Mapping[str, Decoded] | None) -> None:
    assert job.state is not None
    job.state.status = status
    job.state.data = data
    job.state.save(update_fields=["status", "data"])


class Worker:
    def __init__(self, queues: set[Queue]) -> None:
        self._queues = queues
        self._shutdown = False
        self._in_task = False

        # Handle the signals for warm shutdown.
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, sig: int, frame: FrameType | None) -> None:
        if not self._in_task:
            raise InterruptedError
        logger.info("Waiting for active jobs to finish...")
        self._shutdown = True

    def run_job(self, job: Job, task: Task) -> None:
        start_time = time.time()
        job_result = task(job)
        duration = round(time.time() - start_time, 1)
        logger.info(
            f"Done processing job {job}.",
            extra={"duration": duration, "status": job_result["status"], "job": job.as_dict()},
        )
        after_job_finished.send("dreng.worker", job=job, queues=self._queues, duration=timedelta(seconds=duration))

        if job_result["status"] == "succeeded":
            if "state" in job_result:
                _update_state(job, STATUS_SUCCESS, job_result.get("value"))
        elif job_result["status"] == "silent_fail":
            _log_silent_failure(job)
            if "state" in job_result:
                _update_state(job, STATUS_FAILURE, job_result.get("value"))
        elif job_result["status"] == "exception":
            _log_job_exception(job, job_result["exception"])
            if "state" in job_result:
                _update_state(job, STATUS_FAILURE, job_result.get("value"))
            _set_failed(job, "exception")
        elif job_result["status"] == "retried" or job_result["status"] == "expired":
            pass
        else:
            assert_never(job_result["status"])

    def run_once(self) -> DequeueResult:
        with transaction.atomic(durable=True):
            try:
                job = Job.objects.dequeue(self._queues)
            except Job.DoesNotExist:
                return "no_job_to_dequeue"
            logger.info(f"Job {job} was dequeued.", extra={"job": job.as_dict()})
            try:
                task = import_task(job.task)
            except ImportError:
                # This might happen during deploys if an old task runner dequeues a new task.
                logger.warning("Unable to import task", extra={"job": job.as_dict()})
                job.execute_at = timezone.now() + timedelta(seconds=10)
                job.save(force_insert=True)
                return "unable_to_import_task"

            after_task_imported.send("dreng.worker", job=job, queues=self._queues)

            if task.delivery == "at_least_once":
                cache_key = get_claim_count_cache_key(job)
                try:
                    cache_value: int | None = cache.get(cache_key)
                    if cache_value is None:
                        cache.add(cache_key, 1)
                    elif cache_value >= CLAIM_COUNT_LIMIT:
                        _log_claim_count_limit_reached(job, claim_count=cache_value)
                        _set_failed(job, "claim_count_limit_reached")
                        return "claim_count_limit_reached"
                    else:
                        cache.incr(cache_key)
                except CACHE_EXCEPTIONS as e:
                    logger.info(
                        f"Unable to check claim count due to {e.__class__.__name__}.",
                        extra={"exception": str(e), "job": job.as_dict()},
                    )
                    job.execute_at = timezone.now() + timedelta(seconds=10)
                    job.save(force_insert=True)
                    return "unable_to_check_claim_count"
                else:
                    self.run_job(job, task)

        if task.delivery == "at_most_once":
            self.run_job(job, task)

        return "did_dequeue_job"

    def run_available_tasks(self) -> None:
        """
        Runs jobs continuously until there are no more available.
        """
        while True:
            self._in_task = True
            returned = self.run_once()
            self._in_task = False
            if returned in ["no_job_to_dequeue", "unable_to_import_task", "unable_to_check_claim_count"]:
                break

    def run(self) -> None:
        try:
            while True:
                close_old_connections()  # https://code.djangoproject.com/ticket/34914#comment:3

                try:
                    self.run_available_tasks()
                except DATABASE_EXCEPTIONS as e:
                    logger.info(
                        f"Unable to run available jobs due to {e.__class__.__name__}.",
                        extra={"exception": e},
                    )

                if self._shutdown:
                    raise InterruptedError

                time.sleep(1)
        except InterruptedError:
            # got shutdown signal
            pass
