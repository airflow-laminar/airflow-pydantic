from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from importlib.resources import files
from typing import TYPE_CHECKING, Literal

from pydantic import Field, model_validator

from ...core import BaseModel, Dag, TaskArgs
from ...utils import Pool
from ._pool_runtime import reconcile_pools

if TYPE_CHECKING:
    from .balancer import BalancerConfiguration

__all__ = ("PoolManagerConfiguration", "configured_pools")

DEFAULT_DAG_ID = "airflow_laminar_pool_manager"
RUNTIME_IMPORT = "from airflow_pydantic.extras.balancer._pool_runtime import reconcile_pools"
FUTURE_ANNOTATIONS_IMPORT = "from __future__ import annotations\n\n"


def _default_dag() -> Dag:
    return Dag(
        dag_id=DEFAULT_DAG_ID,
        description="Reconcile centrally configured Airflow pools",
        schedule="*/5 * * * *",
        start_date=datetime(2020, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        tags=["airflow-laminar", "pools"],
    )


def _default_task() -> TaskArgs:
    return TaskArgs(pool="default_pool", retries=2)


class PoolManagerConfiguration(BaseModel):
    dag: Dag = Field(default_factory=_default_dag, description="Normal Dag configuration for the generated pool manager")
    task: TaskArgs = Field(default_factory=_default_task, description="Pool reconciliation task options")
    task_id: str = "reconcile_pools"
    backend: Literal["auto", "airflow2", "airflow3", "mwaa"] = "auto"
    connection_id: str = "airflow_laminar_api"
    mwaa_environment_name: str | None = None
    mwaa_region_name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _apply_mapping_defaults(cls, value):
        if not isinstance(value, dict):
            return value

        value = dict(value)
        for field, defaults in (("dag", _default_dag()), ("task", _default_task())):
            configured = value.get(field)
            if isinstance(configured, dict):
                merged = defaults.model_dump(exclude_unset=True)
                merged.update(configured)
                value[field] = merged
        return value

    @model_validator(mode="after")
    def _apply_defaults(self):
        for model, defaults in ((self.dag, _default_dag()), (self.task, _default_task())):
            for field in defaults.model_fields_set - model.model_fields_set:
                setattr(model, field, deepcopy(getattr(defaults, field)))
        return self

    def build_dag(self, config: BalancerConfiguration) -> Dag | None:
        if self.dag.enabled is False:
            return None
        if self.task_id in (self.dag.tasks or {}):
            raise ValueError(f"Pool manager Dag already contains reserved task ID {self.task_id!r}")

        from ...operators.python import PythonTask

        dag = self.dag.model_copy(deep=True)
        dag.tasks = dag.tasks or {}
        dag.tasks[self.task_id] = PythonTask(
            **self.task.model_dump(exclude_unset=True),
            task_id=self.task_id,
            python_callable=reconcile_pools,
            op_kwargs={
                "pool_plan": [
                    {
                        "name": pool.pool,
                        "slots": pool.slots,
                        "description": pool.description,
                        "include_deferred": pool.include_deferred,
                    }
                    for pool in configured_pools(config)
                ],
                "backend": {
                    "kind": self.backend,
                    "connection_id": self.connection_id,
                    "mwaa_environment_name": self.mwaa_environment_name,
                    "mwaa_region_name": self.mwaa_region_name,
                    "override_pool_size": config.override_pool_size,
                },
            },
            do_xcom_push=False,
        )
        return dag

    def generated_files(self, config: BalancerConfiguration) -> dict[str, str]:
        dag = self.build_dag(config)
        if dag is None:
            return {}
        runtime = files("airflow_pydantic.extras.balancer").joinpath("_pool_runtime.py").read_text()
        rendered = dag.render().replace(RUNTIME_IMPORT, runtime.removeprefix(FUTURE_ANNOTATIONS_IMPORT).rstrip())
        return {f"{dag.dag_id}.py": rendered}


def configured_pools(config: BalancerConfiguration) -> list[Pool]:
    pools = []
    for host in config.hosts:
        if host.pool is None:
            raise ValueError(f"Host {host.name} has no configured pool")
        pools.append(
            Pool(
                pool=host.pool.pool,
                slots=host.size,
                description=host.pool.description,
                include_deferred=host.pool.include_deferred,
            )
        )

    for port in config.ports:
        if port.host is None:
            raise ValueError(f"Port {port.port} has no configured host")
        pools.append(
            Pool(
                pool=port.pool,
                slots=1,
                description=f"Balancer pool for host({port.host.name}) port({port.port})",
                include_deferred=True,
            )
        )
    return pools
