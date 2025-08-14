from datetime import datetime
from logging import getLogger
from typing import Any, Dict, List, Optional, Type, Union

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
    trigger_run_id: Optional[str] = Field(default=None, description="The run ID of the DAG run to trigger")
    conf: Optional[Dict[str, Any]] = Field(
        default=None,
        description="A dictionary of configuration parameters to pass to the triggered DAG run",
    )
    logical_date: Optional[Union[datetime, str]] = Field(default=None, description="The logical date of the DAG run to trigger")
    reset_dag_run: Optional[bool] = Field(
        default=None,
        description="Whether clear existing DAG run if already exists. This is useful when backfill or rerun an existing DAG run. This only resets (not recreates) the DAG run. DAG run conf is immutable and will not be reset on rerun of an existing DAG run. When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised. When reset_dag_run=True and dag run exists, existing DAG run will be cleared to rerun.",
    )
    wait_for_completion: Optional[bool] = Field(
        default=None,
        description="Whether or not wait for DAG run completion.",
    )
    poke_interval: Optional[int] = Field(
        default=None,
        description="Poke interval to check DAG run status when wait_for_completion=True)",
    )
    allowed_states: Optional[List[str]] = Field(
        default=None,
        description="Optional list of allowed DAG run states of the triggered DAG. This is useful when setting wait_for_completion to True. Must be a valid DagRunState",
    )
    failed_states: Optional[List[str]] = Field(
        default=None,
        description="Optional list of failed or disallowed DAG run states of the triggered DAG. This is useful when setting wait_for_completion to True. Must be a valid DagRunState",
    )
    skip_when_already_exists: Optional[bool] = Field(
        default=None,
        description="Set to true to mark the task as SKIPPED if a DAG run of the triggered DAG for the same logical date already exists.",
    )
    deferrable: Optional[bool] = Field(
        default=None, description="If waiting for completion, whether or not to defer the task until done, default is False."
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
