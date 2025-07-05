from typing import Optional, Type

from pydantic import Field, field_validator

from ...airflow import PythonOperator
from ...core import Task, TaskArgs
from ...utils import CallablePath
from .airflow_functions import clean_dag_runs, clean_dags

__all__ = (
    "DagClean",
    "DagRunClean",
    "DagCleanOperatorArgs",
    "DagCleanTaskArgs",
    "DagCleanOperator",
    "DagCleanTask",
)


def create_clean_dag_runs():
    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dag_runs(session=None, **context):
        params = context["params"]

        # Get the configurable parameters
        delete_successful = params.get("delete_successful", DagCleanTaskArgs.model_fields["delete_successful"].default)
        delete_failed = params.get("delete_failed", DagCleanTaskArgs.model_fields["delete_failed"].default)
        mark_failed_as_successful = params.get("mark_failed_as_successful", DagCleanTaskArgs.model_fields["mark_failed_as_successful"].default)
        max_dagruns = params.get("max_dagruns", DagCleanTaskArgs.model_fields["max_dagruns"].default)
        days_to_keep = params.get("days_to_keep", DagCleanTaskArgs.model_fields["days_to_keep"].default)

        clean_dag_runs(
            session=session,
            delete_successful=delete_successful,
            delete_failed=delete_failed,
            mark_failed_as_successful=mark_failed_as_successful,
            max_dagruns=max_dagruns,
            days_to_keep=days_to_keep,
        )

    return _clean_dag_runs


def create_clean_dags():
    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dags(session=None, **context):
        clean_dags(session=session)

    return _clean_dags


def create_clean_dags_and_dag_runs():
    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dags_and_dag_runs(session=None, **context):
        clean_dag_runs = create_clean_dag_runs()
        clean_dags = create_clean_dags()
        clean_dag_runs(session=session, **context)
        clean_dags(session=session, **context)

    return _clean_dags_and_dag_runs


class DagRunClean(PythonOperator):
    def __init__(self, **kwargs):
        if "python_callable" in kwargs:
            raise ValueError("DagRunClean does not accept 'python_callable' as an argument.")
        super().__init__(python_callable=create_clean_dag_runs(), **kwargs)


class DagClean(PythonOperator):
    def __init__(self, **kwargs):
        if "python_callable" in kwargs:
            raise ValueError("DagClean does not accept 'python_callable' as an argument.")
        super().__init__(python_callable=create_clean_dags_and_dag_runs(), **kwargs)


class DagCleanTaskArgs(TaskArgs):
    delete_successful: Optional[bool] = Field(default=True)
    delete_failed: Optional[bool] = Field(default=True)
    mark_failed_as_successful: Optional[bool] = Field(default=False)
    max_dagruns: Optional[int] = Field(default=10)
    days_to_keep: Optional[int] = Field(default=10)


# Alias
DagCleanOperatorArgs = DagCleanTaskArgs


class DagCleanTask(Task, DagCleanTaskArgs):
    operator: CallablePath = Field(default="airflow_pydantic.extras.common.clean.DagClean", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if v is not DagClean:
            raise ValueError(f"operator must be 'airflow_pydantic.extras.common.clean.DagClean', got: {v}")
        return v


# Alias
DagCleanOperator = DagCleanTask
