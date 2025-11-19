from __future__ import annotations

import datetime
import logging
import time
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
import time_machine
from django.core.cache import cache
from django.utils import timezone

from dreng.constants import (
    CLAIM_COUNT_LIMIT,
    STATUS_FAILURE,
    STATUS_PENDING,
    STATUS_SUCCESS,
)
from dreng.exceptions import SilentFail
from dreng.models import FailedJob, Job, TaskState
from dreng.utils import get_claim_count_cache_key

if TYPE_CHECKING:
    from dreng.decorators import TaskDecorator
    from dreng.worker import Worker


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("some_task")
class Test_run_available_tasks:
    def test_no_available_tasks(self, worker: Worker) -> None:
        assert not Job.objects.exists()
        worker.run_available_tasks()
        assert not Job.objects.exists()

    def test_stateless_task(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(time_limit=timedelta(seconds=2))
        def stateless_task() -> None:
            pass

        stateless_task.enqueue()
        assert Job.objects.filter(task="worker.stateless_task").first()

        worker.run_available_tasks()
        assert not Job.objects.exists()

    def test_stateful_task(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(keep_state=True, time_limit=timedelta(seconds=2))
        def stateful_task() -> dict[str, str]:
            return {"some": "data"}

        enqueued_task = stateful_task.enqueue()
        assert Job.objects.filter(task="worker.stateful_task").first()

        worker.run_available_tasks()

        assert not Job.objects.exists()

        # error: "EnqueuedTask" has no attribute "state_identifier"  [attr-defined]
        state = TaskState.objects.get(identifier=enqueued_task.state_identifier)  # type: ignore[attr-defined]
        assert state.status == STATUS_SUCCESS
        assert state.data == {"some": "data"}

    def test_silent_fail_stateful_task(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(keep_state=True, time_limit=timedelta(seconds=2))
        def failed_stateful_task() -> None:
            raise SilentFail

        enqueued_task = failed_stateful_task.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()

        # error: "EnqueuedTask" has no attribute "state_identifier"  [attr-defined]
        state = TaskState.objects.get(identifier=enqueued_task.state_identifier)  # type: ignore[attr-defined]
        assert state.status == STATUS_FAILURE
        assert state.data is None

    def test_stateful_task_that_throws(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(keep_state=True, time_limit=timedelta(seconds=2))
        def stateful_task_that_throws() -> None:
            raise AssertionError

        enqueued_task = stateful_task_that_throws.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="worker.stateful_task_that_throws")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == enqueued_task.job.id

        # error: "EnqueuedTask" has no attribute "state_identifier"  [attr-defined]
        state = TaskState.objects.get(identifier=enqueued_task.state_identifier)  # type: ignore[attr-defined]
        assert state.status == STATUS_FAILURE
        assert state.data is None

    def test_silent_fail_stateless_task(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(time_limit=timedelta(seconds=2))
        def failed_stateless_task() -> None:
            raise SilentFail

        failed_stateless_task.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()

    def test_stateless_task_that_throws(self, worker: Worker, mock_task: TaskDecorator) -> None:
        @mock_task(time_limit=timedelta(seconds=2))
        def stateless_task_that_throws() -> None:
            raise AssertionError

        enqueued_task = stateless_task_that_throws.enqueue()
        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="worker.stateless_task_that_throws")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == enqueued_task.job.id

    def test_exception(self, worker: Worker) -> None:
        class FatalError(Exception):
            pass

        with patch("dreng.models.Job.objects.dequeue", side_effect=FatalError), pytest.raises(FatalError):
            worker.run_available_tasks()

    def test_expired_task(self, worker: Worker, mock_task: TaskDecorator, caplog: pytest.LogCaptureFixture) -> None:
        mock = Mock()

        @mock_task(time_limit=timedelta(seconds=2))
        def expired_task() -> None:
            mock.whatever()

        expire_at = timezone.now() - timedelta(minutes=1)
        expired_task.enqueue(expire_at=expire_at)
        job = Job.objects.get(task="worker.expired_task")
        worker.run_available_tasks()

        assert not mock.whatever.called
        assert not Job.objects.exists()

        [expiry_record] = (r for r in caplog.records if r.message == f"Job {job} has expired.")
        assert expiry_record.job["expire_at"] == expire_at  # type: ignore[attr-defined]

    def test_retrying_task(self, worker: Worker, mock_task: TaskDecorator, caplog: pytest.LogCaptureFixture) -> None:
        class RetryError(Exception):
            pass

        @mock_task(retry_exceptions=[RetryError], warn_after_retries=1, time_limit=timedelta(seconds=2))
        def retrying_task() -> None:
            raise RetryError("Retry or the test won't pass.")

        retrying_task.enqueue()
        job = Job.objects.get()
        worker.run_available_tasks()

        retried_job = Job.objects.get(task="worker.retrying_task")
        assert job.id != retried_job.id
        assert retried_job.retry_count == 1
        assert retried_job.execute_at > job.execute_at

        worker.run_available_tasks()

        [expiry_record] = (
            r for r in caplog.records if r.message == f"Job {job} got retry exception. A new job will retry."
        )
        assert expiry_record.old_job["retry_count"] == 0  # type: ignore[attr-defined]
        assert expiry_record.new_job["retry_count"] == 1  # type: ignore[attr-defined]
        assert expiry_record.exception_type == RetryError  # type: ignore[attr-defined]
        assert expiry_record.exception_reason == "Retry or the test won't pass."  # type: ignore[attr-defined]


@pytest.mark.django_db(transaction=True)
def test_task_that_exceeds_its_time_limit(
    worker: Worker, mock_task: TaskDecorator, caplog: pytest.LogCaptureFixture
) -> None:
    # Time limit set to 2 seconds will issue warning at 2 seconds and error at 4 seconds.
    # See the TimeLimit class and the mock_task fixture for details.
    @mock_task(time_limit=timedelta(seconds=2))
    def task_that_exceeds_its_time_limit() -> None:
        time.sleep(5)

    enqueued_task = task_that_exceeds_its_time_limit.enqueue()
    Job.objects.get(task="worker.task_that_exceeds_its_time_limit")

    worker.run_available_tasks()
    log_records = [
        r.message
        for r in caplog.records
        if (
            r.message == "worker.task_that_exceeds_its_time_limit reached its time limit" and r.levelno == logging.ERROR
        )
    ]
    assert len(log_records) == 1

    assert not Job.objects.exists()
    failed_job = FailedJob.objects.get(task="worker.task_that_exceeds_its_time_limit")
    assert failed_job.failure_reason == "exception"
    assert failed_job.job_id == enqueued_task.job.id


@pytest.mark.django_db(transaction=True)
def test_time_limit_warning(worker: Worker, mock_task: TaskDecorator, caplog: pytest.LogCaptureFixture) -> None:
    # Time limit set to 2 seconds will issue warning at 2 seconds and error at 4 seconds.
    # See the TimeLimit class and the mock_task fixture for details.
    @mock_task(time_limit=timedelta(seconds=2))
    def task_that_takes_an_unexpectedly_long_time() -> None:
        time.sleep(3)

    task_that_takes_an_unexpectedly_long_time.enqueue()
    Job.objects.get(task="worker.task_that_takes_an_unexpectedly_long_time")

    worker.run_available_tasks()
    log_records = [
        r.message
        for r in caplog.records
        if (r.message == "Task took an unexpectedly long time" and r.levelno == logging.WARNING)
    ]
    assert len(log_records) == 1

    assert not Job.objects.exists()
    assert not FailedJob.objects.exists()


@pytest.mark.django_db(transaction=True)
class Test_delivery:
    def test_at_least_once(self, worker: Worker, mock_task: TaskDecorator) -> None:
        task_state_pk = uuid.uuid4()

        @mock_task(delivery="at_least_once", time_limit=timedelta(seconds=2))
        def atomic_task() -> None:
            # This could be any operation that committs to the database.
            TaskState.objects.create(identifier=task_state_pk, status=STATUS_PENDING)
            raise AssertionError

        enqueued_task = atomic_task.enqueue()
        job = Job.objects.get(task="worker.atomic_task")

        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="worker.atomic_task")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == job.id == enqueued_task.job.id

        # The task raises an exception, causing a rollback, thus the TaskState is never committed to the database.
        assert not TaskState.objects.exists()

    def test_non_atomic_task(self, worker: Worker, mock_task: TaskDecorator) -> None:
        task_state_pk = uuid.uuid4()

        @mock_task(delivery="at_most_once", time_limit=timedelta(seconds=2))
        def non_atomic_task() -> None:
            # This could be any operation that committs to the database.
            TaskState.objects.create(identifier=task_state_pk, status=STATUS_PENDING)
            raise AssertionError

        enqueued_task = non_atomic_task.enqueue()
        job = Job.objects.get(task="worker.non_atomic_task")

        worker.run_available_tasks()

        assert not Job.objects.exists()
        failed_job = FailedJob.objects.get(task="worker.non_atomic_task")
        assert failed_job.failure_reason == "exception"
        assert failed_job.job_id == job.id == enqueued_task.job.id

        # The task raises an exception but causes no rollback since database operations are committed instantly.
        assert TaskState.objects.get().pk == task_state_pk

    def test_task_killed_by_os(
        self, worker: Worker, mock_task: TaskDecorator, caplog: pytest.LogCaptureFixture
    ) -> None:
        @mock_task(delivery="at_least_once", time_limit=timedelta(seconds=2))
        def stateless_task() -> None:
            raise Exception("Killed by OS")

        job = stateless_task.enqueue().job

        for _ in range(CLAIM_COUNT_LIMIT):
            worker.run_available_tasks()
            # Pretend that stateless_task was killed by OS while running,
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


@pytest.mark.django_db
@pytest.mark.parametrize(
    ["point_in_time", "expected_execute_at"],
    [
        (
            datetime.datetime(2024, 10, 3, 8, tzinfo=ZoneInfo("Europe/Stockholm")),
            datetime.datetime(2024, 10, 3, 10, 30, tzinfo=ZoneInfo("Europe/Stockholm")),
        ),
        (
            datetime.datetime(2024, 10, 3, 12, tzinfo=ZoneInfo("Europe/Stockholm")),
            datetime.datetime(2024, 10, 4, 10, 30, tzinfo=ZoneInfo("Europe/Stockholm")),
        ),
    ],
)
def test_repeat_at(
    worker: Worker, mock_task: TaskDecorator, point_in_time: datetime.datetime, expected_execute_at: datetime.datetime
) -> None:
    @mock_task(delivery="at_least_once", repeat_at=datetime.time(10, 30), time_limit=timedelta(seconds=2))
    def repeating_task() -> None:
        pass

    with time_machine.travel(point_in_time):
        # The default for execute_at is django.contrib.postgres.functions.TransactionNow and
        # Postgres doesn't know that we've travelled in time.
        repeating_task.enqueue(execute_at=point_in_time - timedelta(hours=1))
        initial_job = Job.objects.get()
        assert worker.run_once() == "did_dequeue_job"
        repeating_job = Job.objects.get()

    assert initial_job.id != repeating_job.id
    assert initial_job.task == repeating_job.task
    assert repeating_job.execute_at == expected_execute_at


@pytest.mark.django_db
def test_enqueue_and_execute_evenly(mock_task: TaskDecorator, worker: Worker) -> None:
    task_arguments: list[int] = []

    @mock_task(time_limit=timedelta(seconds=2))
    def some_task(*, single_keyword_argument: int) -> None:
        task_arguments.append(single_keyword_argument)

    # Go back in time to ensure that the worker (that uses SQL-now) will grab the jobs from the queue:
    point_in_time = timezone.now() - timedelta(days=1)

    with time_machine.travel(point_in_time, tick=False):
        some_task.enqueue_and_execute_evenly(
            [{"single_keyword_argument": i} for i in range(4)], span=timedelta(hours=2)
        )

    assert [j.execute_at for j in Job.objects.order_by("execute_at")] == [
        point_in_time,
        point_in_time + timedelta(minutes=30),
        point_in_time + timedelta(minutes=60),
        point_in_time + timedelta(minutes=90),
    ]

    worker.run_available_tasks()

    assert task_arguments == list(range(4))


@pytest.mark.django_db
def test_unable_to_import_task(worker: Worker) -> None:
    initial_execute_at = datetime.datetime(2025, 4, 4, tzinfo=ZoneInfo("Europe/Stockholm"))
    job = Job.objects.create(
        task="not_importable_task",
        queue="tests",
        execute_at=initial_execute_at,
        args=[],
    )
    with time_machine.travel(datetime.datetime(2025, 4, 4, tzinfo=ZoneInfo("Europe/Stockholm")), tick=False):
        assert worker.run_once() == "unable_to_import_task"
    job.refresh_from_db()
    assert job.execute_at == datetime.datetime(2025, 4, 4, 0, 0, 10, tzinfo=ZoneInfo("Europe/Stockholm"))


@pytest.mark.django_db
def test_unable_to_check_claim_count(worker: Worker, mock_task: TaskDecorator) -> None:
    from django.core.cache.backends.base import InvalidCacheBackendError

    @mock_task(delivery="at_least_once", time_limit=timedelta(seconds=2))
    def at_least_once_task() -> None:
        pass

    with (
        time_machine.travel(datetime.datetime(2025, 4, 4, tzinfo=ZoneInfo("Europe/Stockholm")), tick=False),
        patch.object(cache, "get", side_effect=InvalidCacheBackendError()),
    ):
        at_least_once_task.enqueue()
        assert worker.run_once() == "unable_to_check_claim_count"

    job = Job.objects.get()
    assert job.execute_at == datetime.datetime(2025, 4, 4, 0, 0, 10, tzinfo=ZoneInfo("Europe/Stockholm"))
