import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

from airflow_pydantic import BalancerConfiguration, Dag, Host, Pool, PoolManagerConfiguration, Port, TaskArgs
from airflow_pydantic.extras.balancer._pool_runtime import (
    _airflow3_request,
    _mwaa_request,
    _reconcile_airflow2,
    _reconcile_api,
)
from airflow_pydantic.extras.balancer.pool_manager import configured_pools


def test_pool_manager_defaults_and_dag_id_override():
    manager = PoolManagerConfiguration(dag=Dag(dag_id="custom-pools", schedule="0 * * * *"), task=TaskArgs(queue="system"))

    assert manager.dag.dag_id == "custom-pools"
    assert manager.dag.schedule == "0 * * * *"
    assert manager.dag.catchup is False
    assert manager.dag.max_active_runs == 1
    assert manager.dag.is_paused_upon_creation is False
    assert manager.task.queue == "system"
    assert manager.task.pool == "default_pool"
    assert manager.task.retries == 2


def test_pool_manager_dag_options_do_not_require_id_override():
    manager = PoolManagerConfiguration(dag={"schedule": "0 * * * *", "tags": ["platform"]})

    assert manager.dag.dag_id == "airflow_laminar_pool_manager"
    assert manager.dag.schedule == "0 * * * *"
    assert manager.dag.tags == ["platform"]
    assert manager.dag.catchup is False


def test_configured_pools_and_generated_files_are_runtime_independent():
    config = BalancerConfiguration(
        hosts=[Host(name="host", size=4, pool=Pool(pool="host-pool", description="Host pool"))],
        ports=[Port(host_name="host", port=22)],
        pool_manager={"dag": {"dag_id": "custom-pools"}, "backend": "mwaa", "mwaa_environment_name": "environment"},
    )

    assert [pool.model_dump() for pool in configured_pools(config)] == [
        {"pool": "host-pool", "slots": 4, "description": "Host pool", "include_deferred": False},
        {"pool": "host-22", "slots": 1, "description": "Balancer pool for host(host) port(22)", "include_deferred": True},
    ]

    generated = config.generated_files()
    assert set(generated) == {"_airflow_laminar_pool_runtime.py", "custom-pools.py"}
    assert all("airflow_pydantic" not in source for source in generated.values())
    assert '"name": "host-pool"' in generated["custom-pools.py"]
    assert '"mwaa_environment_name": "environment"' in generated["custom-pools.py"]
    for filename, source in generated.items():
        compile(source, filename, "exec")


def test_configuration_does_not_access_airflow_pools():
    with (
        patch("airflow_pydantic.airflow.get_pool") as get_pool,
        patch("airflow_pydantic.airflow.create_or_update_pool") as create_pool,
    ):
        BalancerConfiguration(hosts=[Host(name="host")], ports=[Port(host_name="host", port=22)])

    get_pool.assert_not_called()
    create_pool.assert_not_called()


def test_api_reconciliation_creates_updates_and_preserves_slots():
    existing = {
        "changed": {"name": "changed", "slots": 10, "description": "Old", "include_deferred": True},
        "unchanged": {"name": "unchanged", "slots": 8, "description": "Configured", "include_deferred": False},
    }
    request = MagicMock()

    def request_side_effect(method, path, body=None, missing_ok=False):
        if method == "GET":
            return existing.get(path.rsplit("/", 1)[-1])
        return {}

    request.side_effect = request_side_effect
    plan = [
        {"name": "new", "slots": 2, "description": "New", "include_deferred": False},
        {"name": "changed", "slots": 4, "description": "Configured", "include_deferred": False},
        {"name": "unchanged", "slots": 8, "description": "Configured", "include_deferred": False},
    ]

    actions = _reconcile_api(plan, {"override_pool_size": False}, request)

    assert actions == ["created new", "updated changed: description, include_deferred", "unchanged unchanged"]
    request.assert_any_call("POST", "/pools", body=plan[0])
    request.assert_any_call("PATCH", "/pools/changed", body={"description": "Configured", "include_deferred": False})


def test_airflow2_reconciliation_runs_from_the_task():
    current = MagicMock(slots=10, description="Old", include_deferred=True)
    desired = {"name": "pool", "slots": 4, "description": "Configured", "include_deferred": False}
    pool = MagicMock()
    pool.get_pool.return_value = current
    airflow = ModuleType("airflow")
    models = ModuleType("airflow.models")
    pool_module = ModuleType("airflow.models.pool")
    pool_module.Pool = pool

    with patch.dict(sys.modules, {"airflow": airflow, "airflow.models": models, "airflow.models.pool": pool_module}):
        actions = _reconcile_airflow2([desired], {"override_pool_size": False})

    assert actions == ["updated pool: description, include_deferred"]
    pool.get_pool.assert_called_once_with("pool")
    pool.create_or_update_pool.assert_called_once_with("pool", 10, "Configured", False)


def test_airflow3_backend_uses_task_sdk_connection():
    connection = MagicMock(
        host="airflow-api",
        conn_type="http",
        port=8080,
        extra_dejson={"token": "token"},
    )
    response = MagicMock()
    response.__enter__.return_value.read.return_value = b'{"name": "pool"}'
    connection_api = MagicMock()
    connection_api.get.return_value = connection
    airflow = ModuleType("airflow")
    sdk = ModuleType("airflow.sdk")
    sdk.Connection = connection_api

    with (
        patch.dict(sys.modules, {"airflow": airflow, "airflow.sdk": sdk}),
        patch("airflow_pydantic.extras.balancer._pool_runtime.urlopen", return_value=response) as open_url,
    ):
        request = _airflow3_request({"connection_id": "pool-api"})
        assert request("GET", "/pools/pool") == {"name": "pool"}

    connection_api.get.assert_called_once_with("pool-api")
    api_request = open_url.call_args.args[0]
    assert api_request.full_url == "http://airflow-api:8080/api/v2/pools/pool"
    assert api_request.headers["Authorization"] == "Bearer token"


def test_mwaa_backend_uses_iam_rest_api():
    boto_client = MagicMock()
    boto_client.invoke_rest_api.return_value = {"RestApiStatusCode": 200, "RestApiResponse": {"name": "pool"}}
    boto3 = ModuleType("boto3")
    boto3.client = MagicMock(return_value=boto_client)

    with patch.dict(sys.modules, {"boto3": boto3}):
        request = _mwaa_request({"mwaa_environment_name": "environment", "mwaa_region_name": "us-east-1"})
        assert request("GET", "/pools/pool") == {"name": "pool"}

    boto3.client.assert_called_once_with("mwaa", region_name="us-east-1")
    boto_client.invoke_rest_api.assert_called_once_with(Name="environment", Path="/pools/pool", Method="GET")
