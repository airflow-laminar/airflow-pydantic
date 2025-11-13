from logging import getLogger
from typing import List, Optional, Type

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import DatetimeArg, ImportPath

__all__ = (
    "BranchDateTimeOperatorArgs",
    "BranchDateTimeTaskArgs",
    "BranchDateTimeOperator",
    "BranchDateTimeTask",
)

_log = getLogger(__name__)


class BranchDateTimeTaskArgs(TaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/datetime/index.html#airflow.providers.standard.operators.datetime.BranchDateTimeOperator
    follow_task_ids_if_true: Optional[List[str]] = Field(default=None, description="List of task IDs to follow if condition evaluates to True")
    follow_task_ids_if_false: Optional[List[str]] = Field(default=None, description="List of task IDs to follow if condition evaluates to False")
    target_lower: Optional[DatetimeArg] = Field(default=None, description="The lower bound datetime to compare against")
    target_upper: Optional[DatetimeArg] = Field(default=None, description="The upper bound datetime to compare against")
    use_task_logical_date: Optional[bool] = Field(
        default=None, description="If True, uses the task's logical date for comparison; otherwise, uses the current datetime"
    )
    use_task_execution_date: Optional[bool] = Field(
        default=None, description="If True, uses the task's execution date for comparison; otherwise, uses the current datetime"
    )


# Alias
BranchDateTimeOperatorArgs = BranchDateTimeTaskArgs


class BranchDateTimeTask(Task, BranchDateTimeTaskArgs):
    operator: ImportPath = Field(
        default="airflow_pydantic.airflow.BranchDateTimeOperator", description="airflow operator path", validate_default=True
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import BranchDateTimeOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.datetime.BranchDateTimeOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BranchDateTimeOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BranchDateTimeOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.datetime.BranchDateTimeOperator', got: {v}")
        return v


# Alias
BranchDateTimeOperator = BranchDateTimeTask
