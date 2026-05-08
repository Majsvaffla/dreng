# dreng
A backend for [Django's Tasks framework](https://docs.djangoproject.com/en/6.0/topics/tasks/).

Dreng will run tasks defined by using the `django.tasks.task` decorator but it only supports the basics. To use dreng to it's full extent you'll have to use the `dreng.task` decorator to define your tasks.

## Backend

Dreng only includes a single backend. It requires that the default [database engine is PostgreSQL](https://docs.djangoproject.com/en/6.0/ref/databases/#postgresql-notes) because it relies on [PostgreSQL](https://www.postgresql.org/)-specific features.

## Options

The following keys are supported in the per-backend options dictionary in the [TASKS setting](https://docs.djangoproject.com/en/6.0/ref/settings/#std-setting-TASKS):

- `"default_delivery"`: What strategy (`"at_least_once"` or `"at_most_once"`) to use to deliver tasks. It can be configured per task using the `dreng.task` decorator.
- `"default_time_limits_by_queue"`: Defines default time limits per queue as `dreng.TimeLimit` instances. Time limits are required for tasks defined using the `dreng.task` decorator but disabled for tasks defined by the `django.tasks.task` decorator.
- `"repeating_tasks"`: `set[str]` of dotted paths to tasks that should be enqueued by the `dreng.scripts.start_repeating_tasks` script.
- `"cache_exceptions"`: `set[str]` of dotted paths to exception classes that will be silenced when incrementing the claim count.
- `"database_exceptions"`: `set[str]` of dotted paths to exception classes that will be silenced in the worker main loop.

## Disclaimer

This project is currently undergoing development with expected breaking changes.
