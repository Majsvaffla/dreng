import django.dispatch

on_worker_init = django.dispatch.Signal()
after_task_imported = django.dispatch.Signal()
before_task_run = django.dispatch.Signal()
after_job_finished = django.dispatch.Signal()
