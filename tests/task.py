from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dreng.models import Job

if TYPE_CHECKING:
    from dreng.task import Task


@pytest.mark.django_db
def test_bulk_enqueue(some_task: Task) -> None:
    some_task.bulk_enqueue({"i": i} for i in range(4))
    assert {j.execute_at for j in Job.objects.all()} == {j.execute_at if (j := Job.objects.first()) else None}
    assert {j.args["i"] for j in Job.objects.all()} == {0, 1, 2, 3}
