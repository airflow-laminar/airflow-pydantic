from typing import Dict, List, Optional, Type

from airflow.operators.python import (
    BranchPythonOperator as BaseBranchPythonOperator,
    PythonOperator as BasePythonOperator,
    ShortCircuitOperator as BaseShortCircuitOperator,
)
from pydantic import Field, field_validator

from ..task import Task, TaskArgs
from ..utils import CallablePath, ImportPath

__all__ = (
    "PythonOperatorArgs",
    "PythonTaskArgs",
    "BranchPythonOperatorArgs",
    "BranchPythonTaskArgs",
    "ShortCircuitOperatorArgs",
    "ShortCircuitTaskArgs",
    "PythonOperator",
    "PythonTask",
    "BranchPythonOperator",
    "BranchPythonTask",
    "ShortCircuitOperator",
    "ShortCircuitTask",
)


class PythonTaskArgs(TaskArgs, extra="allow"):
    # python operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator
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
    templates_exts: Optional[List[str]] = Field(
        default=None, description="a list of file extensions to resolve while processing templated fields, for examples ['.sql', '.hql']"
    )
    show_return_value_in_logs: Optional[bool] = Field(
        default=None,
        description="a bool value whether to show return_value logs. Defaults to True, which allows return value log output. It can be set to False",
    )


PythonOperatorArgs = PythonTaskArgs


class BranchPythonTaskArgs(PythonTaskArgs): ...


# Alias
BranchPythonOperatorArgs = BranchPythonTaskArgs


class ShortCircuitTaskArgs(PythonTaskArgs):
    ignore_downstream_trigger_rules: Optional[bool] = Field(
        default=None,
        description=" If set to True, all downstream tasks from this operator task will be skipped. This is the default behavior. If set to False, the direct, downstream task(s) will be skipped but the trigger_rule defined for a other downstream tasks will be respected.",
    )


# Alias
ShortCircuitOperatorArgs = ShortCircuitTaskArgs


class PythonTask(Task, PythonTaskArgs):
    operator: ImportPath = Field(default="airflow.operators.python.PythonOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> ImportPath:
        if not isinstance(v, Type) and issubclass(v, BasePythonOperator):
            raise ValueError(f"operator must be 'airflow.operators.python.PythonOperator', got: {v}")
        return v


# Alias
PythonOperator = PythonTask


class BranchPythonTask(Task, BranchPythonTaskArgs):
    operator: ImportPath = Field(default="airflow.operators.python.BranchPythonOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if not isinstance(v, Type) and issubclass(v, BaseBranchPythonOperator):
            raise ValueError(f"operator must be 'airflow.operators.python.BranchPythonOperator', got: {v}")
        return v


# Alias
BranchPythonOperator = BranchPythonTask


class ShortCircuitTask(Task, ShortCircuitTaskArgs):
    operator: ImportPath = Field(default="airflow.operators.python.ShortCircuitOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if not isinstance(v, Type) and issubclass(v, BaseShortCircuitOperator):
            raise ValueError(f"operator must be 'airflow.operators.python.ShortCircuitOperator', got: {v}")
        return v


# Alias
ShortCircuitOperator = ShortCircuitTask
