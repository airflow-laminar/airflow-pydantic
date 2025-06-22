from importlib.util import find_spec
from logging import getLogger

from airflow.models import DAG as AirflowDAG

__all__ = ("DagInstantiateMixin",)

have_airflow_config = find_spec("airflow_config") is not None

_log = getLogger(__name__)


class DagInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")

        config_instance = kwargs.pop("config", None)
        if config_instance:
            if have_airflow_config:
                from airflow_config import DAG as AirflowConfigDAG, Configuration

                if isinstance(config_instance, Configuration):
                    # If a config instance is provided, we will use the airflow_config DAG wrapper
                    _log.info("Using airflow_config Configuration instance: %s", config_instance)
                    dag_class = AirflowConfigDAG
                else:
                    # Config provided as an argument but wrong type
                    raise TypeError(f"config must be an instance of airflow_config.Configuration, got {type(config_instance)} instead.")
            else:
                # If airflow_config is not available, we will use the Airflow DAG class directly
                _log.warning("airflow_config is not available. Using AirflowDAG directly without configuration support.")
                dag_class = AirflowDAG
        else:
            # If no config instance is provided, we will use the AirflowDAG class directly
            dag_class = AirflowDAG

        # NOTE: accept dag as an argument to allow for instantiation from airflow-config
        dag_instance = kwargs.pop("dag", None)
        if not dag_instance:
            dag_args = self.model_dump(exclude_unset=True, exclude=["type_", "tasks", "dag_id", "enabled"])
            dag_instance = dag_class(dag_id=self.dag_id, **dag_args, **kwargs)

        task_instances = {}

        _log.info("Available tasks: %s", list(self.tasks.keys()))
        with dag_instance:
            _log.info("Instantiating tasks for DAG: %s", self.dag_id)
            # first pass, instantiate all
            for task_id, task in self.tasks.items():
                if not task_id:
                    raise ValueError("task_id must be set to instantiate a task")
                if task_id in task_instances:
                    raise ValueError(f"Duplicate task_id found: {task_id}. Task IDs must be unique within a DAG.")

                _log.info("Instantiating task: %s", task_id)
                _log.info("Task args: %s", task.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]))
                task_instances[task_id] = task.instantiate(**kwargs)

            # second pass, set dependencies
            for task_id, task in self.tasks.items():
                if task.dependencies:
                    for dep in task.dependencies:
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
