from logging import getLogger
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import BashCommands, CallablePath, ImportPath

__all__ = (
    "BashOperatorArgs",
    "BashTaskArgs",
    "BashOperator",
    "BashTask",
)

_log = getLogger(__name__)


class BashTaskArgs(TaskArgs):
    # bash operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html
    bash_command: Union[str, List[str], BashCommands] = Field(default=None, description="bash command string, list of strings, or model")

    env: Optional[Dict[str, str]] = Field(default=None)
    append_env: Optional[bool] = Field(default=None, description="Append environment variables to the existing environment. Default is False")
    output_encoding: Optional[str] = Field(default=None, description="Output encoding for the command, default is 'utf-8'")
    skip_on_exit_code: Optional[int] = Field(default=None, description="Exit code to skip on, default is 99")
    cwd: Optional[str] = Field(default=None)
    output_processor: Optional[CallablePath] = None

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
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import BashOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.operators.bash.BashOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BashOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BashOperator):
            raise ValueError(f"operator must be 'airflow.operators.bash.BashOperator', got: {v}")
        return v


# Alias
BashOperator = BashTask
