from datetime import timedelta
from typing import Literal, Optional, Union

from ..core import TaskArgs

__all__ = ("BaseSensorArgs",)


class BaseSensorArgs(TaskArgs):
    poke_interval: Optional[Union[timedelta, float]] = None
    timeout: Optional[Union[timedelta, float]] = None
    soft_fail: Optional[bool] = None
    mode: Optional[Literal["poke", "reschedule"]] = None
    exponential_backoff: Optional[bool] = None
    max_wait: Optional[Union[timedelta, float]] = None
    silent_fail: Optional[bool] = None
    never_fail: Optional[bool] = None
