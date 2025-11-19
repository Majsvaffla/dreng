import django.dispatch

__all__ = ["after_job_finished", "after_task_imported", "before_task_run", "on_worker_init"]

on_worker_init = django.dispatch.Signal()
after_task_imported = django.dispatch.Signal()
before_task_run = django.dispatch.Signal()
after_job_finished = django.dispatch.Signal()
