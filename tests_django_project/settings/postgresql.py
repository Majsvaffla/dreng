import dj_database_url

from .base import *  # noqa: F403

# Database
# https://docs.djangoproject.com/en/5.2/ref/settings/#databases

DATABASES = {"default": dj_database_url.config(default="postgres:///dreng_tests")}
