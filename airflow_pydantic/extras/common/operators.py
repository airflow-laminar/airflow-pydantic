from typing import Optional

from pydantic import Field

from ...operators import PythonTask, PythonTaskArgs
from ...utils import CallablePath, ImportPath

__all__ = (
    "SkipOperatorArgs",
    "SkipOperator",
    "FailOperatorArgs",
    "FailOperator",
    "PassOperatorArgs",
    "PassOperator",
)


class SkipTaskArgs(PythonTaskArgs):
    python_callable: Optional[CallablePath] = Field(default="airflow_pydantic.skip", description="python_callable")


# Alias
SkipOperatorArgs = SkipTaskArgs


class FailTaskArgs(PythonTaskArgs):
    python_callable: Optional[CallablePath] = Field(default="airflow_pydantic.fail", description="python_callable")


# Alias
FailOperatorArgs = FailTaskArgs


class PassTaskArgs(PythonTaskArgs):
    python_callable: Optional[CallablePath] = Field(default="airflow_pydantic.pass_", description="python_callable")


# Alias
PassOperatorArgs = PassTaskArgs


class SkipTask(PythonTask, SkipOperatorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonOperator", description="airflow operator path", validate_default=True)


# Alias
SkipOperator = SkipTask


class FailTask(PythonTask, FailOperatorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonOperator", description="airflow operator path", validate_default=True)


# Alias
FailOperator = FailTask


class PassTask(PythonTask, PassOperatorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonOperator", description="airflow operator path", validate_default=True)


# Alias
PassOperator = PassTask
