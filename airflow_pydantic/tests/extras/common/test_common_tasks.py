from airflow_pydantic import DagCleanTask


class TestModels:
    def test_dag_clean(self):
        t = DagCleanTask(task_id="test_dag_clean_task")
        assert t
