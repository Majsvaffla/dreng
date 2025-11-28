import logging
from datetime import timedelta

import click

logger = logging.getLogger(__name__)


@click.command
def main() -> None:
    import django
    from django.conf import settings
    from django.core.exceptions import ImproperlyConfigured

    django.setup()

    from dreng.models import Job
    from dreng.utils import import_task

    repeating_tasks = (import_task(task) for task in settings.DRENG_REPEATING_TASKS)

    for task in repeating_tasks:
        if not task.is_repeating:
            raise ImproperlyConfigured(f"{task.import_path} is not a repeating task.")

        if not Job.objects.filter(task=task.import_path).exists():
            assert task.next_execute_at is not None
            execute_at = task.next_execute_at + timedelta(minutes=5)
            task.enqueue(execute_at=execute_at)
            logger.info("Enqueued repeating task", extra={"task": task.import_path, "execute_at": execute_at})


if __name__ == "__main__":
    main()
