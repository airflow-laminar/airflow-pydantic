__all__ = ("DagInstantiateMixin",)

from importlib.metadata import version
from logging import getLogger

from airflow.models import DAG

_log = getLogger(__name__)

if version("apache-airflow") >= "3.0.0":
    _AIRFLOW_3 = True
else:
    _AIRFLOW_3 = False


class DagInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")

        # NOTE: accept dag as an argument to allow for instantiation from airflow-config
        dag_instance = kwargs.pop("dag", None)
        if not dag_instance:
            dag_args = self.model_dump(exclude_unset=True, exclude=["type_", "tasks", "dag_id", "enabled"])
            # Deal with annoying alias issue
            if "schedule" in dag_args and dag_args["schedule"] is None and not _AIRFLOW_3:
                dag_args["schedule_interval"] = dag_args.pop("schedule", None)
            dag_instance = DAG(dag_id=self.dag_id, **dag_args, **kwargs)

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
