from typing import TYPE_CHECKING, Optional, Type

from pydantic import Field, field_validator

from ...core import BaseModel, Task, TaskArgs
from ...utils import CallablePath
from .airflow_functions import cleanup_dag_runs

if TYPE_CHECKING:
    from airflow.providers.standard.operators.python import PythonOperator

__all__ = (
    "DagCleanup",
    "DagCleanupOperatorArgs",
    "DagCleanupTaskArgs",
    "DagCleanupOperator",
    "DagCleanupTask",
)


def create_cleanup_dag_runs():
    from airflow.utils.session import provide_session

    @provide_session
    def _cleanup_dag_runs(session=None, **context):
        params = context["params"]

        # Get the configurable parameters
        delete_successful = params.get("delete_successful", DagCleanupTaskArgs.model_fields["delete_successful"].default)
        delete_failed = params.get("delete_failed", DagCleanupTaskArgs.model_fields["delete_failed"].default)
        mark_failed_as_successful = params.get("mark_failed_as_successful", DagCleanupTaskArgs.model_fields["mark_failed_as_successful"].default)
        max_dagruns = params.get("max_dagruns", DagCleanupTaskArgs.model_fields["max_dagruns"].default)
        days_to_keep = params.get("days_to_keep", DagCleanupTaskArgs.model_fields["days_to_keep"].default)

        cleanup_dag_runs(
            session=session,
            delete_successful=delete_successful,
            delete_failed=delete_failed,
            mark_failed_as_successful=mark_failed_as_successful,
            max_dagruns=max_dagruns,
            days_to_keep=days_to_keep,
        )

    return _cleanup_dag_runs


def create_cleanup_dag_runs_operator(**kwargs) -> "PythonOperator":
    from airflow.providers.standard.operators.python import PythonOperator

    operator = PythonOperator(
        python_callable=create_cleanup_dag_runs(),
        **kwargs,
    )
    return operator


class DagCleanup(BaseModel):
    delete_successful: Optional[bool] = Field(default=True)
    delete_failed: Optional[bool] = Field(default=True)
    mark_failed_as_successful: Optional[bool] = Field(default=False)
    max_dagruns: Optional[int] = Field(default=10)
    days_to_keep: Optional[int] = Field(default=10)

    @property
    def cleanup_dag_runs(self):
        return create_cleanup_dag_runs()


class DagCleanupTaskArgs(DagCleanup, TaskArgs, extra="allow"): ...


# Alias
DagCleanupOperatorArgs = DagCleanupTaskArgs


class DagCleanupTask(Task, DagCleanupTaskArgs):
    operator: CallablePath = Field(default="airflow_pydantic.extras.common.clean.create_cleanup_dag_runs_operator", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if v.__qualname__ != create_cleanup_dag_runs_operator.__qualname__:
            raise ValueError(f"operator must be 'airflow_pydantic.extras.common.clean.create_cleanup_dag_runs_operator', got: {v}")
        return v


# Alias
DagCleanupOperator = DagCleanupTask
