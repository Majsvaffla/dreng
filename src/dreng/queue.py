from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    type Queue = Literal["background", "interactive"]
