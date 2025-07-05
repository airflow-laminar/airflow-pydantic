import pytest

from airflow_pydantic import DagClean


class TestModels:
    def test_dag_clean(self):
        try:
            from airflow import DAG  # noqa: F401
        except ImportError:
            return pytest.skip("Airflow not installed")

        d = DAG(dag_id="test_dag_clean")
        DagClean(task_id="test_clean_dags", dag=d)
