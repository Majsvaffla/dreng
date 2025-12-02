from __future__ import annotations

import datetime
from typing import Any
from zoneinfo import ZoneInfo


def datetime_utc(*args: Any, tzinfo: ZoneInfo = ZoneInfo("UTC"), **kwargs: Any) -> datetime.datetime:
    # "datetime" gets multiple values for keyword argument "tzinfo"
    return datetime.datetime(*args, tzinfo=tzinfo, **kwargs)  # type: ignore[misc]
