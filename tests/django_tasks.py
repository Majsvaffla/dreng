from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from unittest.mock import patch

import django.tasks
import pytest
from django.core.cache import cache

from dreng.constants import CLAIM_COUNT_LIMIT
from dreng.exceptions import SilentFail
from dreng.models import FailedJob, Job
from dreng.utils import get_claim_count_cache_key

from .helpers import datetime_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dreng import PostgreSQLBackend
    from dreng.worker import Worker


@django.tasks.task
def some_task() -> None:
    pass


@django.tasks.task
def failed_task() -> None:
    raise SilentFail


@django.tasks.task
def task_that_throws() -> None:
    raise AssertionError


@pytest.mark.django_db(transaction=True)
class Test_run_available_tasks:
    def test_some_task(self, worker: Worker) -> None:
        some_task.enqueue()
        assert Job.objects.filter(task="tests.django_tasks.some_task").first()

        worker.run_available_tasks()

        assert not Job.objects.exists()

    def test_silent_fail_task(self, worker: Worker) -> None:
        failed_task.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()

    def test_task_that_throws(self, worker: Worker) -> None:
        task_that_throws.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="tests.django_tasks.task_that_throws")
        assert failed_job.failure_reason == "exception"

    def test_exception(self, worker: Worker) -> None:
        class FatalError(Exception):
            pass

        with patch("dreng.models.Job.objects.dequeue", side_effect=FatalError), pytest.raises(FatalError):
            worker.run_available_tasks()


@django.tasks.task
def atomic_task() -> None:
    # This could be any operation that committs to the database.
    Job.objects.create(execute_at=datetime_utc(9999, 12, 31), args={"such": "argument"})
    raise AssertionError


@django.tasks.task
def non_atomic_task() -> None:
    # This could be any operation that committs to the database.
    Job.objects.create(execute_at=datetime_utc(9999, 12, 31), args={"much": "argument"})
    raise AssertionError


@django.tasks.task
def task_killed_by_os() -> None:
    raise Exception("Killed by OS")


@pytest.fixture
def at_most_once_delivery(backend: PostgreSQLBackend) -> Iterator[None]:
    previous_delivery = backend.default_delivery
    backend.default_delivery = "at_most_once"
    yield
    backend.default_delivery = previous_delivery


@pytest.mark.django_db(transaction=True)
class Test_delivery:
    def test_at_least_once(self, worker: Worker) -> None:
        atomic_task.enqueue()
        job = Job.objects.get(task="tests.django_tasks.atomic_task")

        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="tests.django_tasks.atomic_task")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == job.id

        # The task raises an exception, causing a rollback,
        # thus the operations in the task is never committed to the database.
        assert not Job.objects.filter(args={"such": "argument"}).exists()

    @pytest.mark.usefixtures("at_most_once_delivery")
    def test_non_atomic_task(self, worker: Worker) -> None:
        non_atomic_task.enqueue()
        job = Job.objects.get(task="tests.django_tasks.non_atomic_task")

        worker.run_available_tasks()

        assert not Job.objects.filter(task="tests.django_tasks.non_atomic_task").exists()
        failed_job = FailedJob.objects.get(task="tests.django_tasks.non_atomic_task")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == job.id

        # The task raises an exception but causes no rollback since database operations are committed instantly.
        assert Job.objects.filter(args={"much": "argument"}).exists()

    def test_task_killed_by_os(self, worker: Worker, caplog: pytest.LogCaptureFixture) -> None:
        task_killed_by_os.enqueue()
        job = Job.objects.get()

        for _ in range(CLAIM_COUNT_LIMIT):
            worker.run_available_tasks()
            # Pretend that the task was killed by OS while running,
            # for example out of memory, thus not marked as failed.
            job.save(force_insert=True)
            FailedJob.objects.get(job_id=job.id).delete()

        cache_key = get_claim_count_cache_key(job)
        assert cache.get(cache_key) == CLAIM_COUNT_LIMIT
        assert not FailedJob.objects.exists()

        worker.run_available_tasks()

        # There should now be a FailedJob.
        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get()
        assert cache.get(cache_key) == CLAIM_COUNT_LIMIT
        assert failed_job.failure_reason == "claim_count_limit_reached"
        assert failed_job.job_id == job.id
        [log_record] = (r for r in caplog.records if r.message == f"Job {job} hit claim count limit.")
        assert log_record.levelno == logging.WARNING
