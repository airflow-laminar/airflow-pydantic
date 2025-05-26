from typing import Dict, Optional

from pydantic import Field

from ..task import Task, TaskArgs
from ..utils import CallablePath, ImportPath

__all__ = (
    "BashOperatorArgs",
    "BashOperator",
)


class BashOperatorArgs(TaskArgs, extra="allow"):
    # bash operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html
    bash_command: Optional[str] = Field(default=None, description="bash_command")
    env: Optional[Dict[str, str]] = Field(default=None)
    append_env: Optional[bool] = Field(default=False)
    output_encoding: Optional[str] = Field(default="utf-8")
    skip_exit_code: Optional[bool] = Field(default=None)
    skip_on_exit_code: Optional[int] = Field(default=99)
    cwd: Optional[str] = Field(default=None)
    output_processor: Optional[CallablePath] = None


class BashOperator(Task, BashOperatorArgs):
    operator: ImportPath = Field(default="airflow.operators.bash.BashOperator", description="airflow operator path", validate_default=True)
