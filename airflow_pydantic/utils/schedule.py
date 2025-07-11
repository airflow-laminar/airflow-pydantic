from datetime import timedelta
from typing import Literal, Union

from .timetables import (
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
    EventsTimetable,
    MultipleCronTriggerTimetable,
)

__all__ = ("ScheduleArg",)
# TODO
# from airflow.models.dag import ScheduleArg
# ScheduleArg = Union[ArgNotSet, ScheduleInterval, Timetable, BaseDatasetEventInput, Collection["Dataset"]]
# ScheduleInterval = Union[None, str, timedelta, relativedelta]
# ScheduleArg = Union[timedelta, RelativeDelta, Literal["NOTSET"], str, None]
ScheduleArg = Union[
    timedelta,
    Literal["NOTSET"],
    str,
    None,
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    MultipleCronTriggerTimetable,
    EventsTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
]
