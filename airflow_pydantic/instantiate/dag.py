__all__ = ("DagInstantiateMixin",)

from airflow.models import DAG


class DagInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")

        with DAG(
            dag_id=self.dag_id,
            **self.model_dump(exclude_none=True, exclude=["type_", "tasks", "dag_id"]),
            **kwargs,
        ) as dag:
            task_instances = {}

            # first pass, instantiate all
            for task_id, task in self.tasks.items():
                if not task_id:
                    raise ValueError("task_id must be set to instantiate a task")
                task_instances[task_id] = task.instantiate(dag=dag, **kwargs)

            # second pass, set dependencies
            for task_id, task in self.tasks.items():
                for dep in task.dependencies:
                    task_instances[dep] >> task_instances[task_id]

        return dag
