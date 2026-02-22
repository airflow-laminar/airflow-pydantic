from logging import getLogger
from typing import Any

from pydantic import Field, field_validator

from ..core import Task
from ..utils import ImportPath
from .base import BaseSensorArgs

__all__ = (
    "FileSensor",
    "FileSensorArgs",
)

_log = getLogger(__name__)


class FileSensorArgs(BaseSensorArgs):
    # file sensor args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/filesystem/index.html#airflow.providers.standard.sensors.filesystem.FileSensor
    fs_conn_id: str | None = Field(default=None, description="The connection ID to use when connecting to the filesystem")
    filepath: str | None = Field(default=None, description="The file path to check for existence")
    recursive: bool | None = Field(default=None, description="Whether to check for the file recursively in subdirectories")
    deferrable: bool | None = Field(default=None, description="Set to True to enable deferrable mode for this operator")
    start_from_trigger: bool | None = Field(
        default=None, description="If True, the sensor will start from the trigger state when used in deferrable mode"
    )
    trigger_kwargs: dict[str, Any] | None = Field(
        default=None, description="Additional keyword arguments to pass to the trigger when in deferrable mode"
    )


class FileSensor(Task, FileSensorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.FileSensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import FileSensor, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.filesystem.FileSensor', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("FileSensor is a marker class, returning as is")
            return v
        if not issubclass(v, FileSensor):
            raise TypeError(f"operator must be 'airflow.providers.standard.sensors.filesystem.FileSensor', got: {v}")
        return v
