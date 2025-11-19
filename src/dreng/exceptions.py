__all__ = ["SilentFail", "TaskError", "TimeLimitReached"]


class TaskError(Exception):
    pass


class TimeLimitReached(TaskError):
    pass


class SilentFail(TaskError):
    pass
