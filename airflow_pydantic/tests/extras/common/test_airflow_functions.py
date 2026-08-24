from datetime import datetime, timedelta
from urllib.parse import unquote

import pytest
from pytz import UTC

from airflow_pydantic import fail, pass_, skip
from airflow_pydantic.airflow import AirflowFailException, AirflowSkipException
from airflow_pydantic.extras.balancer import _pool_runtime
from airflow_pydantic.extras.common import airflow_functions
from airflow_pydantic.extras.common.airflow_functions import clean_dag_runs_api, clean_dags_api


class TestCommon:
    def test_pass(self):
        pass_()

    def test_skip(self):
        with pytest.raises(AirflowSkipException):
            skip()

    def test_fail(self):
        with pytest.raises(AirflowFailException):
            fail()


class FakeRequest:
    def __init__(self, dags=None, dag_runs=None):
        self.dags = dags or []
        self.dag_runs = dag_runs or {}
        self.deletes = []
        self.patches = []
        self.gets = []

    def __call__(self, method, path, query=None, body=None, missing_ok=False):
        path = unquote(path)
        if method == "DELETE":
            self.deletes.append(path)
            return None
        if method == "PATCH":
            self.patches.append((path, body))
            return None
        self.gets.append((path, query))
        limit = query["limit"]
        offset = query["offset"]
        if path.endswith("/dagRuns"):
            dag_id = path.removeprefix("/dags/").removesuffix("/dagRuns")
            runs = self.dag_runs.get(dag_id, [])
            return {"dag_runs": runs[offset : offset + limit], "total_entries": len(runs)}
        return {"dags": self.dags[offset : offset + limit], "total_entries": len(self.dags)}


def _run(run_id, state, days_old):
    date = (datetime.now(tz=UTC) - timedelta(days=days_old)).isoformat()
    return {"dag_run_id": run_id, "state": state, "logical_date": date, "run_after": date}


def _patch_api(monkeypatch, api):
    monkeypatch.setattr(airflow_functions, "_create_api_request", lambda backend=None: api)


class TestCleanAPI:
    def test_deletes_old_runs_only(self, monkeypatch):
        api = FakeRequest(
            dags=[{"dag_id": "d1", "is_stale": False}],
            dag_runs={"d1": [_run("old", "success", 20), _run("recent", "success", 1)]},
        )
        _patch_api(monkeypatch, api)
        clean_dag_runs_api(delete_successful=True, delete_failed=True, mark_failed_as_successful=False, max_dagruns=10, days_to_keep=10)
        assert api.deletes == ["/dags/d1/dagRuns/old"]
        assert api.patches == []

    def test_deletes_excess_runs(self, monkeypatch):
        runs = [_run(f"r{i}", "success", days_old=5 - i) for i in range(5)]
        api = FakeRequest(dags=[{"dag_id": "d1"}], dag_runs={"d1": runs})
        _patch_api(monkeypatch, api)
        clean_dag_runs_api(delete_successful=True, delete_failed=True, mark_failed_as_successful=False, max_dagruns=2, days_to_keep=10)
        assert api.deletes == [f"/dags/d1/dagRuns/r{i}" for i in range(3)]

    def test_respects_delete_successful_flag(self, monkeypatch):
        api = FakeRequest(
            dags=[{"dag_id": "d1"}],
            dag_runs={"d1": [_run("old_success", "success", 20), _run("old_failed", "failed", 20)]},
        )
        _patch_api(monkeypatch, api)
        clean_dag_runs_api(delete_successful=False, delete_failed=True, mark_failed_as_successful=False, max_dagruns=10, days_to_keep=10)
        assert api.deletes == ["/dags/d1/dagRuns/old_failed"]

    def test_skips_non_deletable_states(self, monkeypatch):
        api = FakeRequest(
            dags=[{"dag_id": "d1"}],
            dag_runs={"d1": [_run("old_running", "running", 20), _run("recent", "success", 1)]},
        )
        _patch_api(monkeypatch, api)
        clean_dag_runs_api(delete_successful=True, delete_failed=True, mark_failed_as_successful=False, max_dagruns=10, days_to_keep=10)
        assert api.deletes == []

    def test_marks_failed_as_successful(self, monkeypatch):
        api = FakeRequest(
            dags=[{"dag_id": "d1"}],
            dag_runs={"d1": [_run("failed", "failed", 1), _run("success", "success", 1)]},
        )
        _patch_api(monkeypatch, api)
        clean_dag_runs_api(delete_successful=True, delete_failed=True, mark_failed_as_successful=True, max_dagruns=10, days_to_keep=10)
        assert api.deletes == []
        assert api.patches == [("/dags/d1/dagRuns/failed", {"state": "success"})]

    def test_paginates(self, monkeypatch):
        api = FakeRequest(dags=[{"dag_id": f"d{i}"} for i in range(150)])
        _patch_api(monkeypatch, api)
        clean_dags_api()
        dag_list_calls = [(path, query) for path, query in api.gets if path == "/dags"]
        assert len(dag_list_calls) == 2
        assert dag_list_calls[0][1] == {"limit": 100, "offset": 0, "exclude_stale": "false"}
        assert dag_list_calls[1][1] == {"limit": 100, "offset": 100, "exclude_stale": "false"}

    def test_clean_dags_deletes_stale_only(self, monkeypatch):
        api = FakeRequest(dags=[{"dag_id": "live", "is_stale": False}, {"dag_id": "stale", "is_stale": True}])
        _patch_api(monkeypatch, api)
        clean_dags_api()
        assert api.deletes == ["/dags/stale"]

    def test_get_api_base_url(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__API__BASE_URL", raising=False)
        monkeypatch.setenv("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", "http://apiserver:8080/execution/")
        assert airflow_functions._get_api_base_url() == "http://apiserver:8080"
        monkeypatch.setenv("AIRFLOW__API__BASE_URL", "http://other:8080/")
        assert airflow_functions._get_api_base_url() == "http://other:8080"
        assert airflow_functions._get_api_base_url("http://explicit:8080/") == "http://explicit:8080"


class TestCreateAPIRequest:
    def test_auto_detects_mwaa_from_environment(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_ENV_NAME", "environment")
        monkeypatch.setattr(_pool_runtime, "_mwaa_request", lambda backend: "mwaa-request")
        assert airflow_functions._create_api_request() == "mwaa-request"

    def test_explicit_mwaa_backend(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_ENV_NAME", raising=False)
        captured = {}

        def mwaa_request(backend):
            captured.update(backend)
            return "mwaa-request"

        monkeypatch.setattr(_pool_runtime, "_mwaa_request", mwaa_request)
        assert airflow_functions._create_api_request({"kind": "mwaa", "mwaa_environment_name": "environment"}) == "mwaa-request"
        assert captured["mwaa_environment_name"] == "environment"

    def test_airflow3_uses_connection(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_ENV_NAME", raising=False)
        monkeypatch.setattr(_pool_runtime, "_airflow3_request", lambda backend: "connection-request")
        assert airflow_functions._create_api_request() == "connection-request"

    def test_airflow3_falls_back_to_environment_token(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_ENV_NAME", raising=False)

        def no_connection(backend):
            raise ValueError("no connection")

        monkeypatch.setattr(_pool_runtime, "_airflow3_request", no_connection)
        monkeypatch.setattr(airflow_functions, "_get_api_base_url", lambda base_url=None: "http://test:8080")
        monkeypatch.setattr(airflow_functions, "_get_api_token", lambda base_url, **kwargs: "test-token")

        calls = []
        monkeypatch.setattr(_pool_runtime, "_http_request", lambda *args, **kwargs: calls.append((args, kwargs)) or {"dags": []})

        request = airflow_functions._create_api_request()
        assert request("GET", "/dags", query={"limit": 1}) == {"dags": []}
        args, kwargs = calls[0]
        assert args == ("http://test:8080", "GET", "/api/v2/dags")
        assert kwargs["query"] == {"limit": 1}
        assert kwargs["token"] == "test-token"
