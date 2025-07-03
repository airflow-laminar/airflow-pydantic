from unittest.mock import MagicMock

import pytest

from airflow_pydantic import DagCleanup


class TestModels:
    def test_dag_cleanup(self):
        try:
            from airflow.models import DagModel  # noqa: F401
        except ImportError:
            return pytest.skip("Airflow DagModel not available in this environment")

        d = DagCleanup()
        d.cleanup_dag_runs(session=MagicMock(), params={})
