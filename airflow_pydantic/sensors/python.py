from logging import getLogger

from pydantic import Field, field_validator

from ..core import Task
from ..utils import CallablePath, ImportPath
from .base import BaseSensorArgs

__all__ = (
    "PythonSensor",
    "PythonSensorArgs",
)

_log = getLogger(__name__)


class PythonSensorArgs(BaseSensorArgs):
    # python sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/python/index.html#airflow.providers.standard.sensors.python.PythonSensor
    python_callable: CallablePath = Field(default=None, description="python_callable")
    op_args: list[object] | None = Field(default=None, description="a list of positional arguments that will get unpacked when calling your callable")
    op_kwargs: dict[str, object] | None = Field(default=None, description="a dictionary of keyword arguments that will get unpacked in your function")
    templates_dict: dict[str, object] | None = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)",
    )


class PythonSensor(Task, PythonSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import PythonSensor, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.python.PythonSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("PythonSensor is a marker class, returning as is")
            return v
        if not issubclass(v, PythonSensor):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.python.PythonSensor', got: {v}")
        return v
