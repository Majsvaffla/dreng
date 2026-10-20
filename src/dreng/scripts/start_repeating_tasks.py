import argparse
from datetime import timedelta

from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from dreng import logging

logger = logging.getLogger(__name__)


def launch(*, backend_alias: str) -> None:
    import django
    from django.core.exceptions import ImproperlyConfigured
    from django.tasks import task_backends

    django.setup()

    from dreng.backends import PostgreSQLBackend
    from dreng.models import Job
    from dreng.utils import import_task

    backend = task_backends[backend_alias]
    assert isinstance(backend, PostgreSQLBackend)
    repeating_tasks = (import_task(task) for task in backend.repeating_tasks)

    for task in repeating_tasks:
        if not task.is_repeating:
            raise ImproperlyConfigured(f"{task.import_path} is not a repeating task.")

        if not Job.objects.filter(task=task.import_path).exists():
            assert task.next_execute_at is not None
            execute_at = task.next_execute_at + timedelta(minutes=5)
            task.enqueue(execute_at=execute_at)
            logger.info("Enqueued repeating task", extra={"task": task.import_path, "execute_at": execute_at})


def main() -> None:
    parser = argparse.ArgumentParser(description="Start all tasks that have been marked as repeating.")
    parser.add_argument("--backend", dest="backend_alias", default=DEFAULT_TASK_BACKEND_ALIAS)
    args = parser.parse_args()  # Enable --help even though it doesn't include much more than the description for now.
    launch(backend_alias=args.backend_alias)
