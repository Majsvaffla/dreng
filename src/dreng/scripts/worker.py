from __future__ import annotations

import argparse

from django import setup
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def launch(queues: set[str]) -> None:
    setup()

    from dreng.signals import on_worker_init

    on_worker_init.send(sender="dreng.worker")

    from dreng.worker import Worker

    for queue in queues:
        if queue not in settings.DRENG_QUEUES:
            raise ImproperlyConfigured(f"{queue} is not defined in settings.")

    Worker(queues).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all available tasks from the queues provided.")
    parser.add_argument("queues", nargs="+")
    args = parser.parse_args()
    launch(set(args.queues))
