# cspell: words SIGALRM
from __future__ import annotations

import contextlib
import datetime
import json
import signal
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypedDict, TypeIs, TypeVar, assert_never, get_args

from django.utils.module_loading import import_string

from . import logging
from .exceptions import TimeLimitReached

__all__ = ["Decoded", "JSONDecoder", "JSONEncoder", "TimeLimit", "get_claim_count_cache_key", "import_task"]

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from types import FrameType

    from .models import Job
    from .task import Task

    DecodedBuiltIn = str | int | float | bool | None
    DecodedNonBuiltIn = datetime.date | datetime.datetime | Decimal
    Decoded = DecodedBuiltIn | DecodedNonBuiltIn | Sequence["Decoded"] | Mapping[str, "Decoded"]

    TDecodedNonBuiltIn = TypeVar("TDecodedNonBuiltIn", bound=DecodedNonBuiltIn)

    class EncodedNonBuiltIn(TypedDict):
        __value__: str
        __type__: EncodedNonBuiltInType

    class AlarmHandler(Protocol):
        def __call__(self, signum: int, frame: FrameType | None) -> None: ...


type EncodedNonBuiltInType = Literal["date", "datetime", "Decimal"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeLimit:
    max: timedelta  # The maximum allowed time limit for task.
    # A factor for how much a task is allowed to exceed it's time limit by before an error is raised.
    excessive_time_factor: float

    def __post_init__(self) -> None:
        if self.excessive_time_factor < 1:
            raise ValueError("excessive_time_factor must be greater than or equal to 1.")

    class __DangerouslyExceedMaximumTimeLimit__(timedelta):
        """
        Instances of this class can be passed to the task decorator argument to override time limit maximum.
        """

    def _set_alarm(self, timeout: int, handler: AlarmHandler) -> None:
        assert timeout > 0
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout)

    @contextlib.contextmanager
    def as_context_manager(self, task: Task, job: Job, limit: timedelta) -> Iterator[TimeLimit]:
        if isinstance(limit, TimeLimit.__DangerouslyExceedMaximumTimeLimit__):
            assert limit > self.max
        elif limit > self.max:
            raise ValueError(f"{limit} is greater than the maximum time limit ({self.max}).")

        limit_seconds = limit.total_seconds()
        error_after = round(limit_seconds * self.excessive_time_factor)
        warning_after = round(limit_seconds)

        def signal_handler_error(signum: int, frame: FrameType | None) -> None:
            raise TimeLimitReached(f"{task.import_path} reached its time limit")

        def signal_handler_warning(signum: int, frame: FrameType | None) -> None:
            logger.warning(
                "Task took an unexpectedly long time",
                extra={
                    "task": task.import_path,
                    "job": job.as_dict(),
                    "error_after_rounded_seconds": error_after,
                    "warning_after_rounded_seconds": warning_after,
                },
            )
            self._set_alarm(error_after - warning_after, signal_handler_error)

        if error_after == warning_after:
            self._set_alarm(error_after, signal_handler_error)
        else:
            self._set_alarm(warning_after, signal_handler_warning)

        try:
            yield self
        finally:
            signal.alarm(0)


def _as_encoded_non_built_in(d: TDecodedNonBuiltIn, encoder: Callable[[TDecodedNonBuiltIn], str]) -> EncodedNonBuiltIn:
    class_name = d.__class__.__name__
    underlying_args = get_args(EncodedNonBuiltInType.__value__)
    assert class_name in underlying_args
    return {
        "__type__": class_name,  # type: ignore[typeddict-item]
        "__value__": encoder(d),
    }


def _encode_non_built_ins(d: DecodedNonBuiltIn) -> EncodedNonBuiltIn:
    if isinstance(d, datetime.datetime):
        return _as_encoded_non_built_in(d, datetime.datetime.isoformat)
    if isinstance(d, datetime.date):
        return _as_encoded_non_built_in(d, datetime.date.isoformat)
    if isinstance(d, Decimal):
        return _as_encoded_non_built_in(d, str)
    assert_never(d)


def _is_encoded_non_built_in(
    d: dict[str, Decoded] | EncodedNonBuiltIn,
) -> TypeIs[EncodedNonBuiltIn]:
    return isinstance(d, dict) and d.keys() == {"__type__", "__value__"}


def _decode_non_built_ins(d: dict[str, Decoded] | EncodedNonBuiltIn) -> dict[str, Decoded] | DecodedNonBuiltIn:
    if _is_encoded_non_built_in(d):
        t, v = d["__type__"], d["__value__"]
        if t == "datetime":
            return datetime.datetime.fromisoformat(v)
        elif t == "date":
            return datetime.date.fromisoformat(v)
        elif t == "Decimal":
            return Decimal(v)
        assert_never(t)
    return d


class JSONEncoder(json.JSONEncoder):
    def __init__(
        self,
        *,
        skipkeys: bool = False,
        ensure_ascii: bool = True,
        check_circular: bool = True,
        allow_nan: bool = True,
        sort_keys: bool = False,
        indent: int | str | None = None,
        separators: tuple[str, str] | None = None,
        default: Callable[..., Any] | None = None,
    ) -> None:
        super().__init__(
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            default=default or _encode_non_built_ins,
        )


class JSONDecoder(json.JSONDecoder):
    def __init__(
        self,
        *,
        object_hook: Callable[[dict[str, Any]], Any] | None = None,
        parse_float: Callable[[str], Any] | None = None,
        parse_int: Callable[[str], Any] | None = None,
        parse_constant: Callable[[str], Any] | None = None,
        strict: bool = True,
        object_pairs_hook: Callable[[list[tuple[str, Any]]], Any] | None = None,
    ) -> None:
        super().__init__(
            object_hook=object_hook or _decode_non_built_ins,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            strict=strict,
            object_pairs_hook=object_pairs_hook,
        )


def import_task(dotted_path: str) -> Task:
    from .task import Task

    task = import_string(dotted_path)
    assert isinstance(task, Task)
    return task


def get_claim_count_cache_key(job: Job) -> str:
    return f"dreng_job_dequeue_count_{job.id}"
