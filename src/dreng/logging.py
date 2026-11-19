import logging
from collections.abc import MutableMapping
from logging import INFO, WARNING
from typing import Any

from django.contrib.postgres.functions import TransactionNow

__all__ = ["INFO", "WARNING", "getLogger"]


class JobArgumentsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        if (job := getattr(record, "job", {})) and isinstance(job, dict):
            # Avoid "overflowing" logging infrastructure:
            if "args" in job:
                job["args"] = str(job["args"])[:1000]
            if "kwargs" in job:
                job["kwargs"] = str(job["kwargs"])[:1000]
        return record


def _transaction_now_to_none(kwargs: dict[str, Any]) -> None:
    for k, v in kwargs.items():
        if isinstance(v, dict):
            _transaction_now_to_none(v)
        elif isinstance(v, TransactionNow):
            kwargs[k] = None


class TransactionNowAdapter(logging.LoggerAdapter[logging.Logger]):
    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        if "extra" in kwargs:
            # Avoid 'TransactionNow()' ending up in logging infrastructure.
            _transaction_now_to_none(kwargs["extra"])
        return msg, kwargs


def getLogger(name: str = "dreng") -> logging.LoggerAdapter[logging.Logger]:
    logger = logging.getLogger(name)
    logger.addFilter(JobArgumentsFilter())
    return TransactionNowAdapter(logger, merge_extra=True)
