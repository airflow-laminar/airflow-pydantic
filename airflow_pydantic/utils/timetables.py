from datetime import timedelta

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
    "CronDataIntervalTimetable",
    "CronTriggerTimetable",
    "DeltaDataIntervalTimetable",
    "DeltaTriggerTimetable",
    "EventsTimetable",
    "FixedTimezone",
    "MultipleCronTriggerTimetable",
    "Timezone",
)

Timezone = TimeZoneName
FixedTimezone = timedelta


class CronTriggerTimetable(BaseModel):
    cron: str
    timezone: str | Timezone | FixedTimezone | None = None
    interval: timedelta | RelativeDelta | None = None
    run_immediately: bool | timedelta | None = None

    def instance(self) -> BaseCronTriggerTimetable:
        return BaseCronTriggerTimetable(**self.model_dump(exclude_unset=True))


class MultipleCronTriggerTimetable(BaseModel):
    crons: list[str]
    timezone: str | Timezone | FixedTimezone
    interval: timedelta | RelativeDelta | None = None
    run_immediately: bool | timedelta | None = None

    def instance(self) -> BaseMultipleCronTriggerTimetable:
        return BaseMultipleCronTriggerTimetable(*self.crons, **self.model_dump(exclude_unset=True, exclude=["crons"]))


class CronDataIntervalTimetable(BaseModel):
    cron: str
    timezone: str | Timezone | FixedTimezone | None = None

    def instance(self) -> BaseCronDataIntervalTimetable:
        return BaseCronDataIntervalTimetable(**self.model_dump(exclude_unset=True))


class DeltaDataIntervalTimetable(BaseModel):
    delta: timedelta | RelativeDelta

    def instance(self) -> BaseDeltaDataIntervalTimetable:
        return BaseDeltaDataIntervalTimetable(**self.model_dump(exclude_unset=True))


class DeltaTriggerTimetable(BaseModel):
    delta: timedelta | RelativeDelta
    interval: timedelta | RelativeDelta | None = None

    def instance(self) -> BaseDeltaTriggerTimetable:
        return BaseDeltaTriggerTimetable(**self.model_dump(exclude_unset=True))


class EventsTimetable(BaseModel):
    event_dates: list[DatetimeArg]
    restrict_to_events: bool | None = False
    presorted: bool | None = False
    description: str | None = None

    def instance(self) -> BaseEventsTimetable:
        return BaseEventsTimetable(**self.model_dump(exclude_unset=True))
