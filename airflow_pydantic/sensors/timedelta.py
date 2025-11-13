from datetime import timedelta
from logging import getLogger
from typing import Optional, Type, Union

from pydantic import Field, field_validator

from ..core import Task
from ..utils import ImportPath
from .base import BaseSensorArgs

__all__ = (
    "TimeDeltaSensorArgs",
    "TimeDeltaSensor",
    "WaitSensorArgs",
    "WaitSensor",
)

_log = getLogger(__name__)


class TimeDeltaSensorArgs(BaseSensorArgs):
    # timedelta sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/time_delta/index.html#airflow.providers.standard.sensors.time_delta.TimeDeltaSensor
    delta: timedelta = Field(default=None, description="Time to wait before succeeding.")
    deferrable: Optional[bool] = Field(default=None, description="If True, the sensor will operate in deferrable mode")


class TimeDeltaSensor(Task, TimeDeltaSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.TimeDeltaSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import TimeDeltaSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.TimeDeltaSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("TimeSensor is a marker class, returning as is")
            return v
        if not issubclass(v, TimeDeltaSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.TimeDeltaSensor', got: {v}")
        return v


class WaitSensorArgs(BaseSensorArgs):
    # wait sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/time_delta/index.html#airflow.providers.standard.sensors.time_delta.WaitSensor
    time_to_wait: Union[timedelta, int] = Field(default=None, description="Time length to wait after the task starts before succeeding.")
    deferrable: Optional[bool] = Field(default=None, description="If True, the sensor will operate in deferrable mode")


class WaitSensor(Task, WaitSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.WaitSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import WaitSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.WaitSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("WaitSensor is a marker class, returning as is")
            return v
        if not issubclass(v, WaitSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.time.WaitSensor', got: {v}")
        return v
