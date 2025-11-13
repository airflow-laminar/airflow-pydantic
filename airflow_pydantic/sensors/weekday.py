from logging import getLogger
from typing import List, Optional, Type

from pydantic import Field, field_validator

from ..core import Task
from ..utils import ImportPath
from .base import BaseSensorArgs

__all__ = (
    "DayOfWeekSensorArgs",
    "DayOfWeekSensor",
)

_log = getLogger(__name__)


class DayOfWeekSensorArgs(BaseSensorArgs):
    # dayofweek sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/weekday/index.html#airflow.providers.standard.sensors.weekday.DayOfWeekSensor
    week_day: List[str] = Field(description="Day of the week to check (full name). Optionally, a set of days can also be provided using a set")
    use_task_logical_date: Optional[bool] = Field(
        default=None,
        description="If True, uses task’s logical date to compare with week_day. Execution Date is Useful for backfilling. If False, uses system’s day of the week. Useful when you don’t want to run anything on weekdays on the system.",
    )

    @field_validator("week_day")
    @classmethod
    def validate_week_day(cls, v: List[str]) -> List[str]:
        valid_days = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
        for day in v:
            if day.title() not in valid_days:
                raise ValueError(f"Invalid day of the week: {day}. Must be one of {valid_days}")
        return v


class DayOfWeekSensor(Task, DayOfWeekSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.DayOfWeekSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import DayOfWeekSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.DayOfWeekSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("DayOfWeekSensor is a marker class, returning as is")
            return v
        if not issubclass(v, DayOfWeekSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.DayOfWeekSensor', got: {v}")
        return v
