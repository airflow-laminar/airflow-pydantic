from logging import getLogger

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath

__all__ = (
    "ExternalTaskMarker",
    "ExternalTaskMarkerArgs",
)

_log = getLogger(__name__)


class ExternalTaskMarkerArgs(TaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/external_task/index.html#airflow.providers.standard.sensors.external_task.ExternalTaskMarker
    ...


class ExternalTaskMarker(Task, ExternalTaskMarkerArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.ExternalTaskMarker", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> ImportPath:
        from airflow_pydantic.airflow import ExternalTaskMarker, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.operators.empty.EmptyOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("EmptyOperator is a marker class, returning as is")
            return v
        if not issubclass(v, ExternalTaskMarker):
            raise TypeError(f"operator must be 'airflow.operators.empty.EmptyOperator', got: {v}")
        return v
