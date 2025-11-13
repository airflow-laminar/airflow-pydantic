from logging import getLogger
from typing import Optional, Type

from pydantic import Field, field_validator

from ..core import Task
from ..utils import DatetimeArg, ImportPath
from .base import BaseSensorArgs

__all__ = (
    "TimeSensorArgs",
    "TimeSensor",
)

_log = getLogger(__name__)


class TimeSensorArgs(BaseSensorArgs):
    # time sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/time/index.html#airflow.providers.standard.sensors.time.TimeSensor
    target_time: Optional[DatetimeArg] = Field(default=None, description="The target date and time to wait for")
    deferrable: Optional[bool] = Field(default=None, description="If True, the sensor will operate in deferrable mode")


class TimeSensor(Task, TimeSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.TimeSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import TimeSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.TimeSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("TimeSensor is a marker class, returning as is")
            return v
        if not issubclass(v, TimeSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.TimeSensor', got: {v}")
        return v
