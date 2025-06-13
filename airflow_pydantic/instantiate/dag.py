__all__ = ("DagInstantiateMixin",)

from airflow.models import DAG


class DagInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")

        # NOTE: accept dag as an argument to allow for instantiation from airflow-config
        dag_instance = kwargs.pop("dag", None)
        if not dag_instance:
            dag_instance = DAG(dag_id=self.dag_id, **self.model_dump(exclude_none=True, exclude=["type_", "tasks", "dag_id"]), **kwargs)

        task_instances = {}

        print("*" * 100)
        print(self.tasks)

        # first pass, instantiate all
        for task_id, task in self.tasks.items():
            if not task_id:
                raise ValueError("task_id must be set to instantiate a task")
            if task_id in task_instances:
                raise ValueError(f"Duplicate task_id found: {task_id}. Task IDs must be unique within a DAG.")
            task_instances[task_id] = task.instantiate(dag=dag_instance, **kwargs)

        # second pass, set dependencies
        for task_id, task in self.tasks.items():
            for dep in task.dependencies:
                task_instances[dep] >> task_instances[task_id]

        return dag_instance
