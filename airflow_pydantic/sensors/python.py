from logging import getLogger
from typing import Dict, List, Optional, Type

from pydantic import Field, field_validator

from ..core import Task
from ..utils import CallablePath, ImportPath
from .base import BaseSensorArgs

__all__ = (
    "PythonSensorArgs",
    "PythonSensor",
)

_log = getLogger(__name__)


class PythonSensorArgs(BaseSensorArgs):
    # python sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/python/index.html#airflow.providers.standard.sensors.python.PythonSensor
    python_callable: CallablePath = Field(default=None, description="python_callable")
    op_args: Optional[List[object]] = Field(
        default=None, description="a list of positional arguments that will get unpacked when calling your callable"
    )
    op_kwargs: Optional[Dict[str, object]] = Field(
        default=None, description="a dictionary of keyword arguments that will get unpacked in your function"
    )
    templates_dict: Optional[Dict[str, object]] = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)",
    )


class PythonSensor(Task, PythonSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import PythonSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.python.PythonSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("PythonSensor is a marker class, returning as is")
            return v
        if not issubclass(v, PythonSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.python.PythonSensor', got: {v}")
        return v
