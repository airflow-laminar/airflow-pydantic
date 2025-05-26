from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Tuple, Union

from airflow.utils.trigger_rule import TriggerRule
from pydantic import AfterValidator
from pytz import timezone

__all__ = (
    "ScheduleArg",
    "DatetimeArg",
    "TriggerRule",
)

# TODO
# from airflow.models.dag import ScheduleArg
# ScheduleArg = Union[ArgNotSet, ScheduleInterval, Timetable, BaseDatasetEventInput, Collection["Dataset"]]
# ScheduleInterval = Union[None, str, timedelta, relativedelta]
# ScheduleArg = Union[timedelta, RelativeDelta, Literal["NOTSET"], str, None]
ScheduleArg = Union[timedelta, Literal["NOTSET"], str, None]


def _datetime_or_datetime_and_timezone(val: Any):
    if isinstance(val, datetime):
        return val
    elif isinstance(val, (tuple,)):
        dt = val[0]
        tz = timezone(val[1])
        dt = dt.replace(tzinfo=tz)
        return dt
    raise ValueError(f"Expected datetime or Dict[str, datetime|str], got {val!r}")


DatetimeArg = Annotated[Union[datetime, Tuple[datetime, str]], AfterValidator(_datetime_or_datetime_and_timezone)]
