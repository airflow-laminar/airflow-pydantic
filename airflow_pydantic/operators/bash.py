from logging import getLogger
from typing import Any

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import BashCommands, CallablePath, ImportPath

__all__ = (
    "BashOperator",
    "BashOperatorArgs",
    "BashTask",
    "BashTaskArgs",
)

_log = getLogger(__name__)


class BashTaskArgs(TaskArgs):
    # bash operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html
    bash_command: str | list[str] | BashCommands = Field(default=None, description="bash command string, list of strings, or model")

    env: dict[str, str] | None = Field(default=None)
    append_env: bool | None = Field(default=None, description="Append environment variables to the existing environment. Default is False")
    output_encoding: str | None = Field(default=None, description="Output encoding for the command, default is 'utf-8'")
    skip_on_exit_code: int | None = Field(default=None, description="Exit code to skip on, default is 99")
    cwd: str | None = Field(default=None)
    output_processor: CallablePath | None = None

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


# Alias
BashOperatorArgs = BashTaskArgs


class BashTask(Task, BashTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.BashOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import BashOperator, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.standard.operators.BashOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BashOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BashOperator):
            raise TypeError(f"operator must be 'airflow.providers.standard.operators.bash.BashOperator', got: {v}")
        return v


# Alias
BashOperator = BashTask
