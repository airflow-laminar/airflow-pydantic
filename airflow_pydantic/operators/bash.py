from importlib.util import find_spec
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator

from ..task import Task, TaskArgs
from ..utils import CallablePath, ImportPath

__all__ = (
    "BashOperatorArgs",
    "BashOperator",
)

have_common_operators = False
if find_spec("airflow_common_operators"):
    have_common_operators = True
    from airflow_common_operators import BashCommands


class BashOperatorArgs(TaskArgs, extra="allow"):
    # bash operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html
    if have_common_operators:
        bash_command: Union[str, List[str], BashCommands] = Field(default=None, description="bash command string, list of strings, or model")
    else:
        bash_command: Union[str, List[str]] = Field(default=None, description="bash command string, list of strings, or model")
    env: Optional[Dict[str, str]] = Field(default=None)
    append_env: Optional[bool] = Field(default=False)
    output_encoding: Optional[str] = Field(default="utf-8")
    skip_exit_code: Optional[bool] = Field(default=None)
    skip_on_exit_code: Optional[int] = Field(default=99)
    cwd: Optional[str] = Field(default=None)
    output_processor: Optional[CallablePath] = None

    @field_validator("bash_command")
    @classmethod
    def validate_bash_command(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v
        elif isinstance(v, list) and have_common_operators and all(isinstance(item, str) for item in v):
            return BashCommands(commands=v)
        elif isinstance(v, list) and all(isinstance(item, str) for item in v):
            return "\n".join(v)
        elif have_common_operators and isinstance(v, BashCommands):
            return v
        else:
            raise ValueError("bash_command must be a string, list of strings, or a BashCommands model")


class BashOperator(Task, BashOperatorArgs):
    operator: ImportPath = Field(default="airflow.operators.bash.BashOperator", description="airflow operator path", validate_default=True)
