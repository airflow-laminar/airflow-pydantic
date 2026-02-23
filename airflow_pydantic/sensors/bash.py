from logging import getLogger
from typing import Any

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import BashCommands, ImportPath

__all__ = (
    "BashSensor",
    "BashSensorArgs",
)

_log = getLogger(__name__)


class BashSensorArgs(TaskArgs):
    # bash sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/bash/index.html#airflow.providers.standard.sensors.bash.BashSensor
    bash_command: str | list[str] | BashCommands = Field(default=None, description="bash command string, list of strings, or model")
    env: dict[str, str] | None = Field(default=None)
    output_encoding: str | None = Field(default=None, description="Output encoding for the command, default is 'utf-8'")
    retry_exit_code: bool | None = Field(default=None)

    @field_validator("bash_command")
    @classmethod
    def validate_bash_command(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v
        elif isinstance(v, list) and all(isinstance(item, str) for item in v):
            return BashCommands(commands=v)
        elif isinstance(v, BashCommands):
            return v
        else:
            raise ValueError("bash_command must be a string, list of strings, or a BashCommands model")


class BashSensor(Task, BashSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.BashSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import BashSensor, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.bash.BashSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BashOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BashSensor):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.bash.BashSensor', got: {v}")
        return v
