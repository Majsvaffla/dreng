from __future__ import annotations

import argparse

from django import setup
from django.core.exceptions import ImproperlyConfigured
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, DEFAULT_TASK_QUEUE_NAME


def launch(*, queues: set[str], backend_alias: str) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all available tasks from the queues provided.")
    parser.add_argument("queues", nargs="+", default=[DEFAULT_TASK_QUEUE_NAME])
    parser.add_argument("--backend", dest="backend_alias", default=DEFAULT_TASK_BACKEND_ALIAS)
    args = parser.parse_args()
    launch(queues=set(args.queues), backend_alias=args.backend_alias)
