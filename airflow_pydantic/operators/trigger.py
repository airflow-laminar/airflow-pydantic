from logging import getLogger
from typing import Any, Dict, Optional, Type

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath

__all__ = (
    "TriggerDagRunOperatorArgs",
    "TriggerDagRunOperator",
    "TriggerDagRunTaskArgs",
    "TriggerDagRunTask",
)

_log = getLogger(__name__)


class TriggerDagRunTaskArgs(TaskArgs):
    trigger_dag_id: str = Field(description="The DAG ID of the DAG to trigger")
    conf: Optional[Dict[str, Any]] = Field(
        default=None,
        description="A dictionary of configuration parameters to pass to the triggered DAG run",
    )


TriggerDagRunOperatorArgs = TriggerDagRunTaskArgs


class TriggerDagRunTask(Task, TriggerDagRunTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.TriggerDagRunOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> ImportPath:
        from airflow_pydantic.airflow import TriggerDagRunOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.operators.python.TriggerDagRunOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("TriggerDagRunOperator is a marker class, returning as is")
            return v
        if not issubclass(v, TriggerDagRunOperator):
            raise ValueError(f"operator must be 'airflow.operators.python.TriggerDagRunOperator', got: {v}")
        return v


# Alias
TriggerDagRunOperator = TriggerDagRunTask
