from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Literal

__all__ = [
    "CLAIM_COUNT_LIMIT",
    "DEFAULT_WARN_RETRIES",
    "STATUS_FAILURE",
    "STATUS_PENDING",
    "STATUS_SUCCESS",
    "Delivery",
    "Priority",
    "TaskStateStatus",
]

if TYPE_CHECKING:
    type TaskStateStatus = Literal["pending", "success", "failure"]

    """
    A task can be delivered in two different ways, either to be run at least once or at most once,
    in case of abnormal interruptions that prevents it from finishing successfully.

    "at_least_once" implies that the task is atomic, i.e. runs in a single database transaction.
    It guarantees that the task is run if it is dequeued and ensures that it is put back into the
    queue if it doesn't finish successfully. If the database transaction within which the task runs
    isn't committed, the task will be dequeued and run again. This kind of task must be safe to run
    multiple times if the database transaction isn't committed. Thus, it shouldn't have any side
    effects that aren't part of the database transaction.

    "at_most_once" neither implies that the task is atomic nor does it guarantee that the task is
    run. In case of interruptions after it has been dequeued, it won't be put back into the queue.
    This kind of task usually has side effects that can't be committed to the database, such as
    sending an e-mail or sending a request to an external API.
    """
    type Delivery = Literal["at_least_once", "at_most_once"]


DEFAULT_WARN_RETRIES = 10

STATUS_PENDING: TaskStateStatus = "pending"
STATUS_SUCCESS: TaskStateStatus = "success"
STATUS_FAILURE: TaskStateStatus = "failure"


class Priority(Enum):
    none = 0
    low = 11
    medium = 22
    high = 33


CLAIM_COUNT_LIMIT = 1
