from __future__ import annotations

from typing import TYPE_CHECKING

from django.contrib import admin, messages
from django.db.models import QuerySet, TextChoices
from django.urls import reverse
from django.utils import timezone

from . import logging
from .constants import Priority
from .models import BaseJob, FailedJob, Job
from .utils import import_task

__all__ = ["FailedJobAdmin", "JobAdmin"]

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from django.http import HttpRequest

logger = logging.getLogger(__name__)


@admin.display(ordering="priority")
def priority(job: Job) -> str:
    return f"{Priority(job.priority).name} ({job.priority})"


def _format_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@admin.display(ordering="execute_at")
def execute_at(job: Job) -> str:
    return _format_datetime(job.execute_at)


@admin.display(ordering="created_at")
def created_at(job: Job) -> str:
    return _format_datetime(job.created_at)


@admin.display
def queue(job: Job | FailedJob) -> str:
    return import_task(job.task).queue


class QueueFilter[T: FailedJob | Job](admin.SimpleListFilter):
    title = "queue"
    parameter_name = "queue"

    def lookups(self, request: HttpRequest, model_admin: admin.ModelAdmin[BaseJob]) -> tuple[tuple[str, str], ...]:
        return (
            ("background", "background"),
            ("interactive", "interactive"),
        )

    def queryset(self, request: HttpRequest, queryset: QuerySet[T]) -> QuerySet[T]:
        if self.value() is None:
            return queryset
        return queryset.filter(pk__in=(j.pk for j in queryset if import_task(j.task).queue == self.value()))


class ExecuteAtFilter[T: FailedJob | Job](admin.SimpleListFilter):
    title = "execute at"
    parameter_name = "execute_at"

    class Choices(TextChoices):
        past = "past", "past"
        future = "future", "future"

    def lookups(self, request: HttpRequest, model_admin: admin.ModelAdmin[Job | FailedJob]) -> list[tuple[str, str]]:
        return self.Choices.choices

    def queryset(self, request: HttpRequest, queryset: QuerySet[T]) -> QuerySet[T]:
        if self.value() == self.Choices.past:
            return queryset.filter(execute_at__lt=timezone.now())
        if self.value() == self.Choices.future:
            return queryset.filter(execute_at__gte=timezone.now())
        return queryset


class JobAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
    ordering = ["execute_at", "priority"]
    editable = False
    list_filter = ("priority", QueueFilter, ExecuteAtFilter)
    list_display_links = ("task",)
    readonly_fields = [
        priority,  # type: ignore[list-item]
        "created_at",
        "task",
        "args",
        queue,  # type: ignore[list-item]
    ]

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Job | None = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Job | None = None) -> bool:
        return True

    def get_list_display(self, request: HttpRequest) -> list[str | Callable[[Job], str | bool]]:
        return [
            execute_at,
            "task",
            priority,
            created_at,
            queue,
        ]

    def get_fields(
        self, request: HttpRequest, job: Job | None = None
    ) -> (
        list[str | list[str] | tuple[str, ...] | tuple[()]]
        | tuple[str | list[str] | tuple[str, ...] | tuple[()], ...]
        | tuple[()]
    ):
        assert job
        return [
            "created_at",
            "execute_at",
            priority,  # type: ignore[list-item]
            "task",
            "args",
            queue,  # type: ignore[list-item]
        ]


admin.site.register(Job, JobAdmin)


@admin.display(ordering="failed_at")
def failed_at(job: FailedJob) -> str:
    return _format_datetime(job.failed_at)


@admin.action(description="Re-run failed jobs (enqueue a new task with the same arguments)")
def rerun_failed_job(modeladmin: FailedJobAdmin, request: HttpRequest, queryset: QuerySet[FailedJob]) -> None:
    for job in queryset:
        job.rerun()
    messages.info(request, f"{len(queryset)} tasks have been enqueued.")


class FailedJobAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
    ordering = ["failed_at"]
    editable = False
    list_filter = ("task", "failure_reason", QueueFilter)
    list_display_links = ("task",)
    actions = [rerun_failed_job]
    readonly_fields = [
        "task",
        "args",
        "state",
        "failed_at",
        "failure_reason",
        "job_id",
    ]

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: FailedJob | None = None) -> bool:
        # Only allow change when responding to deletion since that will
        # redirect with filters, messages and everything to the changelist view.
        return request.method == "POST" and obj is not None and request.path == reverse("admin:dreng", args=[obj.id])

    def has_delete_permission(self, request: HttpRequest, obj: FailedJob | None = None) -> bool:
        return True

    def get_list_display(self, request: HttpRequest) -> list[str | Callable[[FailedJob], str | bool]]:
        return [
            failed_at,
            "task",
            "failure_reason",
            "job_id",
        ]

    def get_fields(
        self, request: HttpRequest, job: FailedJob | None = None
    ) -> (
        list[str | list[str] | tuple[str, ...] | tuple[()]]
        | tuple[str | list[str] | tuple[str, ...] | tuple[()], ...]
        | tuple[()]
    ):
        assert job
        return [
            "failed_at",
            "task",
            "args",
            "failure_reason",
            "job_id",
            "state",
        ]

    def delete_model(self, request: HttpRequest, job: FailedJob) -> None:
        logger.info("Delete failed job manually", extra={"failed_job": job.as_dict()})
        return super().delete_model(request, job)

    def delete_queryset(self, request: HttpRequest, queryset: QuerySet[FailedJob]) -> None:
        logger.info(
            "Delete failed jobs manually", extra={"failed_job_ids": list(queryset.values_list("id", flat=True))}
        )
        return super().delete_queryset(request, queryset)


admin.site.register(FailedJob, FailedJobAdmin)
