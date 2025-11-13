from logging import getLogger
from typing import Any, Dict, Optional, Type

from pydantic import Field, field_validator

from ..core import Task
from ..utils import DatetimeArg, ImportPath
from .base import BaseSensorArgs

__all__ = (
    "DateTimeSensorArgs",
    "DateTimeSensor",
    "DateTimeSensorAsyncArgs",
    "DateTimeSensorAsync",
)

_log = getLogger(__name__)


class DateTimeSensorArgs(BaseSensorArgs):
    # datetime sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/date_time/index.html#airflow.providers.standard.sensors.date_time.DateTimeSensor
    target_time: Optional[DatetimeArg] = Field(default=None, description="The target date and time to wait for")


class DateTimeSensor(Task, DateTimeSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.DateTimeSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import DateTimeSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.date_time.DateTimeSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("DateTimeSensor is a marker class, returning as is")
            return v
        if not issubclass(v, DateTimeSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.date_time.DateTimeSensor', got: {v}")
        return v


class DateTimeSensorAsyncArgs(BaseSensorArgs):
    # datetime sensor async args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/date_time/index.html#airflow.providers.standard.sensors.date_time.DateTimeSensorAsync
    target_time: Optional[DatetimeArg] = Field(default=None, description="The target date and time to wait for")
    start_from_trigger: Optional[bool] = Field(
        default=None, description="If True, the sensor will start from the trigger state when used in deferrable mode"
    )
    trigger_kwargs: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional keyword arguments to pass to the trigger when in deferrable mode"
    )
    end_from_trigger: Optional[bool] = Field(
        default=None, description="If True, the sensor will end from the trigger state when used in deferrable mode"
    )


class DateTimeSensorAsync(Task, DateTimeSensorAsyncArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.DateTimeSensorAsync", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import DateTimeSensorAsync, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.date_time.DateTimeSensorAsync', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("DateTimeSensorAsync is a marker class, returning as is")
            return v
        if not issubclass(v, DateTimeSensorAsync):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.date_time.DateTimeSensorAsync', got: {v}")
        return v
