import pytest

from airflow_pydantic import DagClean
from airflow_pydantic.migration import _airflow_3


class TestModels:
    def test_dag_clean(self):
        try:
            from airflow import DAG
        except ImportError:
            return pytest.skip("Airflow not installed")

        d = DAG(dag_id="test_dag_clean")
        DagClean(task_id="test_clean_dags", dag=d)

    @pytest.mark.skipif(not _airflow_3(), reason="Airflow 3 only")
    def test_clean_callables_use_api_on_airflow_3(self, monkeypatch):
        from airflow_pydantic.extras.common import clean

        dag_run_calls = []
        dag_calls = []
        monkeypatch.setattr(clean, "clean_dag_runs_api", lambda **kwargs: dag_run_calls.append(kwargs))
        monkeypatch.setattr(clean, "clean_dags_api", lambda **kwargs: dag_calls.append(kwargs))

        clean.create_clean_dags_and_dag_runs()(params={"days_to_keep": 3, "mwaa_environment_name": "environment"})

        backend = {
            "kind": "auto",
            "connection_id": "airflow_laminar_api",
            "mwaa_environment_name": "environment",
            "mwaa_region_name": None,
        }
        assert dag_run_calls == [
            {
                "delete_successful": True,
                "delete_failed": True,
                "mark_failed_as_successful": False,
                "max_dagruns": 10,
                "days_to_keep": 3,
                "backend": backend,
            }
        ]
        assert dag_calls == [{"backend": backend}]
