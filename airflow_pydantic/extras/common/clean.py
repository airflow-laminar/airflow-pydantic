from typing import Literal

from pydantic import Field, field_validator

from ...airflow import PythonOperator
from ...core import Task, TaskArgs
from ...migration import _airflow_3
from ...utils import CallablePath
from .airflow_functions import clean_dag_runs, clean_dag_runs_api, clean_dags, clean_dags_api

__all__ = (
    "DagClean",
    "DagCleanOperator",
    "DagCleanOperatorArgs",
    "DagCleanTask",
    "DagCleanTaskArgs",
    "DagRunClean",
)


def _resolve_clean_params(context):
    params = context["params"]
    return {
        "delete_successful": params.get("delete_successful", DagCleanTaskArgs.model_fields["delete_successful"].default),
        "delete_failed": params.get("delete_failed", DagCleanTaskArgs.model_fields["delete_failed"].default),
        "mark_failed_as_successful": params.get("mark_failed_as_successful", DagCleanTaskArgs.model_fields["mark_failed_as_successful"].default),
        "max_dagruns": params.get("max_dagruns", DagCleanTaskArgs.model_fields["max_dagruns"].default),
        "days_to_keep": params.get("days_to_keep", DagCleanTaskArgs.model_fields["days_to_keep"].default),
    }


def _resolve_backend(context):
    params = context["params"]
    return {
        "kind": params.get("backend") or "auto",
        "connection_id": params.get("connection_id") or DagCleanTaskArgs.model_fields["connection_id"].default,
        "mwaa_environment_name": params.get("mwaa_environment_name"),
        "mwaa_region_name": params.get("mwaa_region_name"),
    }


def create_clean_dag_runs():
    if _airflow_3():
        # Airflow 3 tasks have no database access, use the REST API
        def _clean_dag_runs(**context):
            clean_dag_runs_api(backend=_resolve_backend(context), **_resolve_clean_params(context))

        return _clean_dag_runs

    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dag_runs(session=None, **context):
        clean_dag_runs(session=session, **_resolve_clean_params(context))

    return _clean_dag_runs


def create_clean_dags():
    if _airflow_3():
        # Airflow 3 tasks have no database access, use the REST API
        def _clean_dags(**context):
            clean_dags_api(backend=_resolve_backend(context))

        return _clean_dags

    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dags(session=None, **context):
        clean_dags(session=session)

    return _clean_dags


def create_clean_dags_and_dag_runs():
    if _airflow_3():

        def _clean_dags_and_dag_runs(**context):
            create_clean_dag_runs()(**context)
            create_clean_dags()(**context)

        return _clean_dags_and_dag_runs

    # Wrapped to avoid airflow imports
    from airflow.utils.session import provide_session

    @provide_session
    def _clean_dags_and_dag_runs(session=None, **context):
        clean_dag_runs = create_clean_dag_runs()
        clean_dags = create_clean_dags()
        clean_dag_runs(session=session, **context)
        clean_dags(session=session, **context)

    return _clean_dags_and_dag_runs


def _move_clean_kwargs_to_params(kwargs):
    clean_params = {}
    for key in (
        "delete_successful",
        "delete_failed",
        "mark_failed_as_successful",
        "max_dagruns",
        "days_to_keep",
        "backend",
        "connection_id",
        "mwaa_environment_name",
        "mwaa_region_name",
    ):
        if key in kwargs:
            clean_params[key] = kwargs.pop(key)
    if clean_params:
        kwargs["params"] = {**clean_params, **(kwargs.get("params") or {})}


class DagRunClean(PythonOperator):
    def __init__(self, **kwargs):
        if "python_callable" in kwargs:
            raise ValueError("DagRunClean does not accept 'python_callable' as an argument.")
        _move_clean_kwargs_to_params(kwargs)
        super().__init__(python_callable=create_clean_dag_runs(), **kwargs)


class DagClean(PythonOperator):
    def __init__(self, **kwargs):
        if "python_callable" in kwargs:
            raise ValueError("DagClean does not accept 'python_callable' as an argument.")
        _move_clean_kwargs_to_params(kwargs)
        super().__init__(python_callable=create_clean_dags_and_dag_runs(), **kwargs)


class DagCleanTaskArgs(TaskArgs):
    delete_successful: bool | None = Field(default=True)
    delete_failed: bool | None = Field(default=True)
    mark_failed_as_successful: bool | None = Field(default=False)
    max_dagruns: int | None = Field(default=10)
    days_to_keep: int | None = Field(default=10)
    backend: Literal["auto", "airflow3", "mwaa"] | None = Field(default="auto", description="API backend used on Airflow 3")
    connection_id: str | None = Field(default="airflow_laminar_api", description="Airflow connection with API credentials for the airflow3 backend")
    mwaa_environment_name: str | None = Field(default=None, description="MWAA environment name for the mwaa backend")
    mwaa_region_name: str | None = Field(default=None, description="AWS region of the MWAA environment")


# Alias
DagCleanOperatorArgs = DagCleanTaskArgs


class DagCleanTask(Task, DagCleanTaskArgs):
    operator: CallablePath = Field(default="airflow_pydantic.extras.common.clean.DagClean", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        if v is not DagClean:
            raise ValueError(f"operator must be 'airflow_pydantic.extras.common.clean.DagClean', got: {v}")
        return v


# Alias
DagCleanOperator = DagCleanTask
