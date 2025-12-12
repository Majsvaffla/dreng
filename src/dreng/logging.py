import logging
from logging import INFO, WARNING

__all__ = ["INFO", "WARNING", "getLogger"]


class JobArgsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        if (job := getattr(record, "job", {})) and isinstance(job, dict) and "args" in job:
            job["args"] = str(job["args"])[:1000]  # Avoid "overflowing" logging infrastructure.
        return record


def getLogger(name: str = "dreng") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addFilter(JobArgsFilter())
    return logger
