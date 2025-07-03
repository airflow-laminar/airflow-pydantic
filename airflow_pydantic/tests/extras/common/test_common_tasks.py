import pytest

from airflow_pydantic import DagCleanupTask


class TestModels:
    def test_dag_cleanup(self):
        try:
            from airflow import DAG
        except ImportError:
            return pytest.skip("Airflow DAG not available in this environment")

        d = DAG(dag_id="test_dag_cleanup")
        t = DagCleanupTask(dag=d, task_id="test_dag_cleanup_task")
        assert t
