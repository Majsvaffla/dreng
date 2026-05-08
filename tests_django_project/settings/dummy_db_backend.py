from .base import *  # noqa: F403

# Database
# https://docs.djangoproject.com/en/5.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "tests_django_project.dummy_db_backend",
        "NAME": "dreng_tests",
    }
}

TASKS = {
    "default": {
        "BACKEND": "dreng.PostgreSQLBackend",
        "OPTIONS": {
            "database_exceptions": {
                "django.db.OperationalError",
            },
        },
    },
}
