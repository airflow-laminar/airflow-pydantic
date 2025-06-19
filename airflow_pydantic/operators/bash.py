from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator

from ..task import Task, TaskArgs
from ..utils import BashCommands, CallablePath, ImportPath

__all__ = (
    "BashOperatorArgs",
    "BashSensorArgs",
    "BashOperator",
    "BashSensor",
)


class BashOperatorArgs(TaskArgs, extra="allow"):
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


class BashSensorArgs(TaskArgs, extra="allow"):
    # bash sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/bash/index.html#airflow.providers.standard.sensors.bash.BashSensor
    bash_command: Union[str, List[str], BashCommands] = Field(default=None, description="bash command string, list of strings, or model")
    env: Optional[Dict[str, str]] = Field(default=None)
    output_encoding: Optional[str] = Field(default=None, description="Output encoding for the command, default is 'utf-8'")
    retry_exit_code: Optional[bool] = Field(default=None)

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


class BashOperator(Task, BashOperatorArgs):
    operator: ImportPath = Field(default="airflow.operators.bash.BashOperator", description="airflow operator path", validate_default=True)


class BashSensor(Task, BashSensorArgs):
    operator: ImportPath = Field(default="airflow.sensors.bash.BashSensor", description="airflow sensor path", validate_default=True)
