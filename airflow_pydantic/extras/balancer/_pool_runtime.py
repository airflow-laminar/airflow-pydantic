from __future__ import annotations

import json
from importlib.metadata import version
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

POOL_RUNTIME_VERSION = 1


def reconcile_pools(pool_plan: list[dict[str, Any]], backend: dict[str, Any]) -> list[str]:
    kind = backend.get("kind", "auto")
    if kind == "auto":
        if backend.get("mwaa_environment_name"):
            kind = "mwaa"
        elif int(version("apache-airflow").split(".", 1)[0]) < 3:
            kind = "airflow2"
        else:
            kind = "airflow3"

    if kind == "airflow2":
        actions = _reconcile_airflow2(pool_plan, backend)
    elif kind == "airflow3":
        actions = _reconcile_api(pool_plan, backend, _airflow3_request(backend))
    elif kind == "mwaa":
        actions = _reconcile_api(pool_plan, backend, _mwaa_request(backend))
    else:
        raise ValueError(f"Unsupported pool manager backend: {kind}")

    for action in actions:
        print(action)
    return actions


def _reconcile_airflow2(pool_plan: list[dict[str, Any]], backend: dict[str, Any]) -> list[str]:
    from airflow.models.pool import Pool

    actions = []
    for desired in pool_plan:
        try:
            current = Pool.get_pool(desired["name"])
        except Exception as error:  # Airflow versions expose different PoolNotFound classes.
            if error.__class__.__name__ != "PoolNotFound":
                raise
            current = None

        if current is None:
            Pool.create_or_update_pool(
                desired["name"],
                desired["slots"],
                desired["description"],
                desired["include_deferred"],
            )
            actions.append(f"created {desired['name']}")
            continue

        values = _changes(current, desired, backend.get("override_pool_size", False))
        if values:
            Pool.create_or_update_pool(
                desired["name"],
                values.get("slots", current.slots),
                values.get("description", current.description),
                values.get("include_deferred", current.include_deferred),
            )
            actions.append(f"updated {desired['name']}: {', '.join(values)}")
        else:
            actions.append(f"unchanged {desired['name']}")
    return actions


def _reconcile_api(pool_plan: list[dict[str, Any]], backend: dict[str, Any], request) -> list[str]:
    actions = []
    for desired in pool_plan:
        name = desired["name"]
        current = request("GET", f"/pools/{quote(name, safe='')}", missing_ok=True)
        if current is None:
            request("POST", "/pools", body=desired)
            actions.append(f"created {name}")
            continue

        values = _changes(current, desired, backend.get("override_pool_size", False))
        if values:
            request("PATCH", f"/pools/{quote(name, safe='')}", body=values)
            actions.append(f"updated {name}: {', '.join(values)}")
        else:
            actions.append(f"unchanged {name}")
    return actions


def _changes(current: Any, desired: dict[str, Any], override_pool_size: bool) -> dict[str, Any]:
    def get(name: str, default=None):
        return current.get(name, default) if isinstance(current, dict) else getattr(current, name, default)

    changes = {}
    if override_pool_size and get("slots") != desired["slots"]:
        changes["slots"] = desired["slots"]
    if get("description") != desired["description"]:
        changes["description"] = desired["description"]
    if get("include_deferred", False) != desired["include_deferred"]:
        changes["include_deferred"] = desired["include_deferred"]
    return changes


def _airflow3_request(backend: dict[str, Any]):
    from airflow.sdk import Connection

    connection = Connection.get(backend.get("connection_id", "airflow_laminar_api"))
    extra = connection.extra_dejson
    base_url = extra.get("base_url")
    if not base_url:
        if not connection.host:
            raise ValueError("Airflow pool manager connection requires a host or extra.base_url")
        host = connection.host.rstrip("/")
        base_url = host if "://" in host else f"{connection.conn_type or 'http'}://{host}"
        if connection.port:
            base_url = f"{base_url}:{connection.port}"

    token = extra.get("token")
    if not token:
        response = _http_request(
            base_url,
            "POST",
            "/auth/token",
            body={"username": connection.login, "password": connection.password},
        )
        token = response["access_token"]

    def request(method: str, path: str, body=None, missing_ok: bool = False):
        return _http_request(base_url, method, f"/api/v2{path}", body=body, token=token, missing_ok=missing_ok)

    return request


def _http_request(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    token: str | None = None,
    missing_ok: bool = False,
):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read())
    except HTTPError as error:
        if missing_ok and error.code == 404:
            return None
        raise RuntimeError(f"Airflow API request failed with HTTP {error.code}: {error.read().decode()}") from error


def _mwaa_request(backend: dict[str, Any]):
    import boto3

    environment_name = backend.get("mwaa_environment_name")
    if not environment_name:
        raise ValueError("mwaa_environment_name is required for the MWAA pool manager backend")
    client = boto3.client("mwaa", region_name=backend.get("mwaa_region_name"))

    def request(method: str, path: str, body=None, missing_ok: bool = False):
        kwargs = {"Name": environment_name, "Path": path, "Method": method}
        if body is not None:
            kwargs["Body"] = body
        response = client.invoke_rest_api(**kwargs)
        status = response["RestApiStatusCode"]
        if missing_ok and status == 404:
            return None
        if not 200 <= status < 300:
            raise RuntimeError(f"MWAA REST API request failed with HTTP {status}: {response['RestApiResponse']}")
        result = response.get("RestApiResponse")
        return json.loads(result) if isinstance(result, str) else result

    return request
