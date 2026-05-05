from __future__ import annotations

import click
from django import setup
from django.core.exceptions import ImproperlyConfigured
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, DEFAULT_TASK_QUEUE_NAME


@click.argument("queues", nargs=-1, default=[DEFAULT_TASK_QUEUE_NAME])
@click.option("--backend", "backend_alias", default=DEFAULT_TASK_BACKEND_ALIAS)
@click.command
def main(queues: set[str], backend_alias: str) -> None:
    setup()

    from dreng.signals import on_worker_init

    on_worker_init.send(sender="dreng.worker")

    from django.tasks import task_backends

    from dreng.backends import PostgreSQLBackend

    backend = task_backends[backend_alias]
    assert isinstance(backend, PostgreSQLBackend)

    for queue in queues:
        if queue not in backend.queues:
            raise ImproperlyConfigured(f"{queue} is not defined for the {backend.alias} backend.")

    from dreng.worker import Worker

    Worker(queues, backend).run()


if __name__ == "__main__":
    main()
