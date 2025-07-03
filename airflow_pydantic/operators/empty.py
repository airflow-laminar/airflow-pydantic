from logging import getLogger
from typing import Type

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath

__all__ = (
    "EmptyOperatorArgs",
    "EmptyOperator",
    "EmptyTask",
)

_log = getLogger(__name__)


class EmptyTaskArgs(TaskArgs): ...


EmptyOperatorArgs = EmptyTaskArgs


class EmptyTask(Task, EmptyTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.EmptyOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> ImportPath:
        from airflow_pydantic.airflow import EmptyOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.operators.empty.EmptyOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("EmptyOperator is a marker class, returning as is")
            return v
        if not issubclass(v, EmptyOperator):
            raise ValueError(f"operator must be 'airflow.operators.empty.EmptyOperator', got: {v}")
        return v


# Alias
EmptyOperator = EmptyTask
