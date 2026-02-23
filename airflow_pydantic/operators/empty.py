from logging import getLogger

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath

__all__ = (
    "EmptyOperator",
    "EmptyOperatorArgs",
    "EmptyTask",
)

_log = getLogger(__name__)


class EmptyTaskArgs(TaskArgs): ...


EmptyOperatorArgs = EmptyTaskArgs


class EmptyTask(Task, EmptyTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.EmptyOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> ImportPath:
        from airflow_pydantic.airflow import EmptyOperator, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.standard.operators.empty.EmptyOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("EmptyOperator is a marker class, returning as is")
            return v
        if not issubclass(v, EmptyOperator):
            raise TypeError(f"operator must be 'airflow.providers.standard.operators.empty.EmptyOperator', got: {v}")
        return v


# Alias
EmptyOperator = EmptyTask
