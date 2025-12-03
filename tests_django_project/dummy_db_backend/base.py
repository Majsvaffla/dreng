from functools import cached_property

import django.db.backends.postgresql.base
from django.db import OperationalError
from django.db.utils import DatabaseErrorWrapper


class DatabaseWrapper(django.db.backends.postgresql.base.DatabaseWrapper):
    @cached_property
    def wrap_database_errors(self) -> DatabaseErrorWrapper:
        raise OperationalError("This is a dummy database wrapper that only raises this exception.")
