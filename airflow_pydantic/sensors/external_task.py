from datetime import timedelta
from logging import getLogger
from typing import List, Optional, Type, Union

from pydantic import Field, field_validator

from ..core import Task
from ..utils import CallablePath, ImportPath
from .base import BaseSensorArgs

__all__ = (
    "ExternalTaskSensorArgs",
    "ExternalTaskSensor",
)

_log = getLogger(__name__)


class ExternalTaskSensorArgs(BaseSensorArgs):
    # external task sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/external_task/index.html#airflow.providers.standard.sensors.external_task.ExternalTaskSensor
    external_dag_id: Optional[str] = Field(default=None, description="The dag_id of the external DAG to monitor")
    external_task_id: Optional[str] = Field(default=None, description="The task_id of the external task to monitor")
    external_task_ids: Optional[List[str]] = Field(default=None, description="A list of task_ids of the external tasks to monitor")
    external_task_group_id: Optional[str] = Field(default=None, description="The task group ID of the external tasks to monitor")
    allowed_states: Optional[List[str]] = Field(
        default=None, description="A list of allowed states for the external task(s) to be considered successful"
    )
    skipped_states: Optional[List[str]] = Field(default=None, description="A list of states for the external task(s) to be considered skipped")
    failed_states: Optional[List[str]] = Field(default=None, description="A list of states for the external task(s) to be considered failed")
    execution_delta: Optional[timedelta] = Field(
        default=None, description="A timedelta to add to the current task's execution date to determine the external task's execution date"
    )
    execution_date_fn: Optional[CallablePath] = Field(
        default=None, description="A callable that takes the current task's execution date and returns the external task's execution date"
    )
    check_existence: Optional[bool] = Field(
        default=None, description="If True, the sensor will check for the existence of the external DAG and task(s) before monitoring"
    )
    poll_interval: Optional[Union[timedelta, float]] = Field(
        default=None, description="Time in seconds or timedelta to wait between each poke to check the external task's state"
    )
    deferrable: Optional[bool] = Field(default=None, description="Set to True to enable deferrable mode for this operator")


class ExternalTaskSensor(Task, ExternalTaskSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.ExternalTaskSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import ExternalTaskSensor, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.external_task.ExternalTaskSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("ExternalTaskSensor is a marker class, returning as is")
            return v
        if not issubclass(v, ExternalTaskSensor):
            raise ValueError(f"operator must be 'airflow.providers.standard.sensors.external_task.ExternalTaskSensor', got: {v}")
        return v
