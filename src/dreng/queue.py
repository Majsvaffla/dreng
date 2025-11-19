from __future__ import annotations

from typing import TYPE_CHECKING, Literal

__all__ = ["Queue"]

if TYPE_CHECKING:
    type Queue = Literal["background", "interactive"]
