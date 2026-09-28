import subprocess
import sys


def test_airflow_3_import_does_not_load_local_api_client():
    script = """
import importlib.abc
import sys

class BlockLocalApiClient(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "airflow.api.client":
            raise ImportError("local API client is unavailable")
        return None

sys.meta_path.insert(0, BlockLocalApiClient())
import airflow_pydantic.airflow
"""

    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
