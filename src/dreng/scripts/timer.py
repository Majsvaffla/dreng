import time
from datetime import timedelta

import click

from dreng import Priority, task


@task(
    priority=Priority.high,
    keep_state=True,
    delivery="at_least_once",
    queue="interactive",
    time_limit=timedelta(minutes=2),
)
def measure_time(*, start_time: float) -> dict[str, float]:
    return {"time": time.time() - start_time}


@click.command
def main() -> None:
    from dreng.constants import STATUS_PENDING, STATUS_SUCCESS
    from dreng.models import TaskState

    while True:
        enqueued_task = measure_time.enqueue(start_time=time.time())
        state = TaskState.objects.active().get(identifier=enqueued_task.state_identifier)
        while state.status == STATUS_PENDING:
            state = TaskState.objects.active().get(identifier=enqueued_task.state_identifier)
        assert state.status == STATUS_SUCCESS, "Task failed!"
        assert state.data is not None
        seconds: float = state.data["time"]
        if seconds >= 1.0:
            pass
        time.sleep(1)


if __name__ == "__main__":
    main()
