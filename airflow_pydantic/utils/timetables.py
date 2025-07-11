from datetime import timedelta
from typing import List, Optional, Union

from pydantic_extra_types.timezone_name import TimeZoneName

from ..airflow import (
    CronDataIntervalTimetable as BaseCronDataIntervalTimetable,
    CronTriggerTimetable as BaseCronTriggerTimetable,
    DeltaDataIntervalTimetable as BaseDeltaDataIntervalTimetable,
    DeltaTriggerTimetable as BaseDeltaTriggerTimetable,
    EventsTimetable as BaseEventsTimetable,
    MultipleCronTriggerTimetable as BaseMultipleCronTriggerTimetable,
)
from ..core import BaseModel
from .common import DatetimeArg
from .relativedelta import RelativeDelta

__all__ = (
    "Timezone",
    "FixedTimezone",
    "CronTriggerTimetable",
    "CronDataIntervalTimetable",
    "DeltaDataIntervalTimetable",
    "DeltaTriggerTimetable",
    "EventsTimetable",
    "MultipleCronTriggerTimetable",
)

Timezone = TimeZoneName
FixedTimezone = timedelta


class CronTriggerTimetable(BaseModel):
    cron: str
    timezone: Optional[Union[str, Timezone, FixedTimezone]] = None
    interval: Optional[Union[timedelta, RelativeDelta]] = None
    run_immediately: Optional[Union[bool, timedelta]] = None

    def instance(self) -> BaseCronTriggerTimetable:
        return BaseCronTriggerTimetable(**self.model_dump(exclude_unset=True))


class MultipleCronTriggerTimetable(BaseModel):
    crons: List[str]
    timezone: Union[str, Timezone, FixedTimezone]
    interval: Optional[Union[timedelta, RelativeDelta]] = None
    run_immediately: Optional[Union[bool, timedelta]] = None

    def instance(self) -> BaseMultipleCronTriggerTimetable:
        return BaseMultipleCronTriggerTimetable(*self.crons, **self.model_dump(exclude_unset=True, exclude=["crons"]))


class CronDataIntervalTimetable(BaseModel):
    cron: str
    timezone: Optional[Union[str, Timezone, FixedTimezone]] = None

    def instance(self) -> BaseCronDataIntervalTimetable:
        return BaseCronDataIntervalTimetable(**self.model_dump(exclude_unset=True))


class DeltaDataIntervalTimetable(BaseModel):
    delta: Union[timedelta, RelativeDelta]

    def instance(self) -> BaseDeltaDataIntervalTimetable:
        return BaseDeltaDataIntervalTimetable(**self.model_dump(exclude_unset=True))


class DeltaTriggerTimetable(BaseModel):
    delta: Union[timedelta, RelativeDelta]
    interval: Optional[Union[timedelta, RelativeDelta]] = None

    def instance(self) -> BaseDeltaTriggerTimetable:
        return BaseDeltaTriggerTimetable(**self.model_dump(exclude_unset=True))


class EventsTimetable(BaseModel):
    event_dates: List[DatetimeArg]
    restrict_to_events: Optional[bool] = False
    presorted: Optional[bool] = False
    description: Optional[str] = None

    def instance(self) -> BaseEventsTimetable:
        return BaseEventsTimetable(**self.model_dump(exclude_unset=True))
