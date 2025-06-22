from datetime import timedelta
from typing import Any, Dict, List, Literal, Optional, Type, Union

from airflow.sensors.bash import BashSensor as BaseBashSensor
from airflow.sensors.python import PythonSensor as BasePythonSensor
from pydantic import Field, field_validator

from ..task import Task, TaskArgs
from ..utils import BashCommands, CallablePath, ImportPath

__all__ = (
    "BashSensorArgs",
    "BashSensor",
    "PythonSensorArgs",
    "PythonSensor",
)


class BaseSensorArgs(TaskArgs, extra="allow"):
    poke_interval: Optional[Union[timedelta, float]] = None
    timeout: Optional[Union[timedelta, float]] = None
    soft_fail: Optional[bool] = None
    mode: Optional[Literal["poke", "reschedule"]] = None
    exponential_backoff: Optional[bool] = None
    max_wait: Optional[Union[timedelta, float]] = None
    silent_fail: Optional[bool] = None
    never_fail: Optional[bool] = None


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
    operator: ImportPath = Field(default="airflow.sensors.python.PythonSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if not isinstance(v, Type) and issubclass(v, BasePythonSensor):
            raise ValueError(f"operator must be 'airflow.sensors.python.PythonSensor', got: {v}")
        return v


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


class BashSensor(Task, BashSensorArgs):
    operator: ImportPath = Field(default="airflow.sensors.bash.BashSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if not isinstance(v, Type) and issubclass(v, BaseBashSensor):
            raise ValueError(f"operator must be 'airflow.sensors.bash.BashSensor', got: {v}")
        return v
