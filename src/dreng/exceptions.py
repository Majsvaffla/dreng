class TaskError(Exception):
    pass


class TimeLimitReached(TaskError):
    pass


class SilentFail(TaskError):
    pass
