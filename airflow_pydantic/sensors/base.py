from datetime import timedelta
from typing import Literal

from ..core import TaskArgs

__all__ = ("BaseSensorArgs",)


class BaseSensorArgs(TaskArgs):
    poke_interval: timedelta | float | None = None
    timeout: timedelta | float | None = None
    soft_fail: bool | None = None
    mode: Literal["poke", "reschedule"] | None = None
    exponential_backoff: bool | None = None
    max_wait: timedelta | float | None = None
    silent_fail: bool | None = None
    never_fail: bool | None = None
