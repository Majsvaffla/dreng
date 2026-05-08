from __future__ import annotations

import click
from django import setup
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


@click.argument("queues", nargs=-1, required=True)
@click.command
def main(queues: set[str]) -> None:
    setup()

    from dreng.signals import on_worker_init

    on_worker_init.send(sender="dreng.worker")

    from dreng.worker import Worker

    for queue in queues:
        if queue not in settings.DRENG_QUEUES:
            raise ImproperlyConfigured(f"{queue} is not defined in settings.")

    Worker(queues).run()


if __name__ == "__main__":
    main()
