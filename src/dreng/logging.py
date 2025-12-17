import logging
from logging import INFO, WARNING

from django.contrib.postgres.functions import TransactionNow

__all__ = ["INFO", "WARNING", "getLogger"]


class JobArgsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        if (job := getattr(record, "job", {})) and isinstance(job, dict):
            if args := job.get("args"):
                job["args"] = str(args)[:1000]  # Avoid "overflowing" logging infrastructure.
            if (created_at := job.get("created_at")) and isinstance(created_at, TransactionNow):
                job["created_at"] = None  # Avoid 'TransactionNow()' ending up in logging infrastructure.
            if (execute_at := job.get("execute_at")) and isinstance(execute_at, TransactionNow):
                job["execute_at"] = None  # Avoid 'TransactionNow()' ending up in logging infrastructure.
        return record


def getLogger(name: str = "dreng") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addFilter(JobArgsFilter())
    return logger
