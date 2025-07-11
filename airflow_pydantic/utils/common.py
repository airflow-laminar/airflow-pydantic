from datetime import datetime
from typing import Annotated, Any, Tuple, Union

from pendulum import datetime as pendulum_datetime
from pydantic import AfterValidator

from ..airflow import TriggerRule

__all__ = (
    "DatetimeArg",
    "TriggerRule",
)


def _datetime_or_datetime_and_timezone(val: Any):
    if isinstance(val, datetime):
        return val
    elif isinstance(val, (tuple,)):
        dt = val[0]
        dt = pendulum_datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, tz=val[1])
        return dt
    raise ValueError(f"Expected datetime or Dict[str, datetime|str], got {val!r}")


DatetimeArg = Annotated[Union[datetime, Tuple[datetime, str]], AfterValidator(_datetime_or_datetime_and_timezone)]
