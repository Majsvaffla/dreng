from __future__ import annotations

import datetime
from typing import Any
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")


def datetime_utc(*args: Any, tzinfo: ZoneInfo = UTC, **kwargs: Any) -> datetime.datetime:
    # "datetime" gets multiple values for keyword argument "tzinfo"
    return datetime.datetime(*args, tzinfo=tzinfo, **kwargs)  # type: ignore[misc]
