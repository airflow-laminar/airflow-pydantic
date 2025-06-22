from importlib.util import find_spec
from logging import getLogger

from airflow.models import DAG as AirflowDAG
from pydantic import BaseModel

__all__ = ("DagInstantiateMixin",)

have_airflow_config = find_spec("airflow_config") is not None

_log = getLogger(__name__)


class DagInstantiateMixin:
    def instantiate(self: BaseModel, **kwargs):
        # NOTE: accept dag as an argument
        dag_instance = kwargs.pop("dag", None)
        config_instance = kwargs.pop("config", None)

        if dag_instance and config_instance:
            raise ValueError(
                "Cannot provide both 'dag' and 'config' arguments.Please provide only one as Configration instance will override the DAG instance."
            )

        if not self.dag_id and dag_instance:
            # If a DAG instance is provided, we will use its dag_id
            self.dag_id = dag_instance.dag_id
        elif not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")

        if config_instance:
            # NOTE: If airflow_config is available, we will use it to instantiate the DAG
            # Note that airflow_config.Configuration.apply is going to re-call this method,
            # so we want to take care to not do everything twice.
            if have_airflow_config:
                from airflow_config import DAG as AirflowConfigDAG, Configuration

                if isinstance(config_instance, Configuration):
                    # If a config instance is provided, we will use the airflow_config DAG wrapper
                    _log.info("Using airflow_config.Configuration instance: %s", config_instance)

                    # Config instance is applied last and overrides `self`, so update ourselves if
                    # we find ourselves in the DAG and log info aobut it
                    if self.dag_id in config_instance.dags:
                        _log.info("DAG %s found in airflow_config.Configuration instance, applying its settings.", self.dag_id)
                        config_dag = config_instance.dags[self.dag_id]
                        for k, v in config_dag.model_dump(exclude_unset=True, exclude_defaults=True, exclude=["type_", "tasks"]).items():
                            _log.info("DAG %s overriding %s with value: %s", self.dag_id, k, v)
                            setattr(self, k, v)

                        # Finish here and let the config instance handle the instantiation
                        if not dag_instance:
                            dag_instance = AirflowConfigDAG(dag_id=self.dag_id, config=config_instance)
                else:
                    # Config provided as an argument but wrong type
                    raise TypeError(f"config must be an instance of airflow_config.Configuration, got {type(config_instance)} instead.")
            else:
                # If airflow_config is not available, we will use the Airflow DAG class directly
                _log.warning("airflow_config is not available. Using AirflowDAG directly without configuration support.")

        # NOTE: accept dag as an argument to allow for instantiation from airflow-config
        if not dag_instance:
            dag_instance = AirflowDAG(
                dag_id=self.dag_id, **self.model_dump(exclude_unset=True, exclude=["type_", "tasks", "dag_id", "enabled"]), **kwargs
            )

        task_instances = {}

        with dag_instance:
            _log.info("Available tasks: %s\nInstantiating tasks for DAG: %s", list(self.tasks.keys()), self.dag_id)
            # first pass, instantiate all
            for task_id, task in self.tasks.items():
                if not task_id:
                    raise ValueError("task_id must be set to instantiate a task")
                if task_id in task_instances:
                    raise ValueError(f"Duplicate task_id found: {task_id}. Task IDs must be unique within a DAG.")

                _log.info(
                    "Instantiating task: %s\nTask args: %s",
                    task_id,
                    task.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]),
                )
                task_instances[task_id] = task.instantiate(dag=dag_instance, **kwargs)

            # second pass, set dependencies
            for task_id, task in self.tasks.items():
                if task.dependencies:
                    for dep in task.dependencies:
                        if isinstance(dep, tuple):
                            dep, attr = dep
                            _log.info("Setting dependency: %s.%s >> %s", dep, attr, task_id)
                            getattr(task_instances[dep], attr) >> task_instances[task_id]
                        else:
                            _log.info("Setting dependency: %s >> %s", dep, task_id)
                            task_instances[dep] >> task_instances[task_id]
            return dag_instance

    def __enter__(self):
        """
        Allows the DagInstantiateMixin to be used as a context manager.
        """
        with self.instantiate() as dag_instance:
            return dag_instance

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Allows the DagInstantiateMixin to be used as a context manager.
        """
        if exc_type is not None:
            _log.error("An error occurred during DAG instantiation: %s", exc_value)
        return False
