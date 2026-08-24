import os
from datetime import datetime, timedelta
from logging import getLogger
from urllib.parse import quote

from pytz import UTC

from ...airflow import AirflowFailException, AirflowSkipException

__all__ = (
    "clean_dag_runs",
    "clean_dag_runs_api",
    "clean_dags",
    "clean_dags_api",
    "fail",
    "pass_",
    "skip",
)

_log = getLogger(__name__)


def skip():
    _log.info("Skipping task execution")
    raise AirflowSkipException


def fail():
    _log.info("Failing task execution")
    raise AirflowFailException


def pass_():
    _log.info("Passing task execution")


def clean_dag_runs(session, delete_successful, delete_failed, mark_failed_as_successful, max_dagruns, days_to_keep):
    from airflow.models import DagModel, DagRun
    from airflow.utils.state import State

    # Make cutoff_date timezone-aware (UTC)
    utc_now = datetime.now(tz=UTC)
    cutoff_date = utc_now - timedelta(days=days_to_keep)
    _log.info(f"Cutoff date for clean: {cutoff_date}")

    # Fetch all DAGs from the DagBag
    dag_ids = [d.dag_id for d in session.query(DagModel.dag_id).distinct(DagModel.dag_id).all()]
    _log.info(f"Found DAGs to clean up: {dag_ids}")

    deleted = 0

    for dag_id in dag_ids:
        _log.info(f"Cleaning up DAG: {dag_id}")

        # Query for DAG runs of each DAG
        query = session.query(DagRun).filter(DagRun.dag_id == dag_id)

        if delete_successful is False:
            _log.info(f"Not deleting successful DAG runs for DAG: {dag_id}")
            query = query.filter(DagRun.state != State.SUCCESS)
        if delete_failed is False:
            _log.info(f"Not deleting failed DAG runs for DAG: {dag_id}")
            query = query.filter(DagRun.state != State.FAILED)

        dagruns = query.order_by(DagRun.execution_date.asc()).all()
        total_runs = len(dagruns)
        _log.info(f"Found {total_runs} DAG runs to clean up for DAG: {dag_id}")

        for dr in dagruns:
            # Compare execution_date (offset-aware) with cutoff_date (now offset-aware)
            if dr.execution_date < cutoff_date or total_runs > max_dagruns:
                _log.info(f"Deleting DAG run: {dr}")
                session.delete(dr)
                deleted += 1
                total_runs -= 1  # Adjust count since we deleted one
            elif mark_failed_as_successful:
                # Need to iterate through all remaining
                if dr.state == State.FAILED:
                    # Mark failed runs as successful
                    _log.info(f"Marking failed DAG run as successful: {dr}")
                    dr.state = State.SUCCESS
                    session.merge(dr)
            elif not mark_failed_as_successful:
                break  # Since they are ordered, no more to delete

    _log.info("Committing DAG run deletions")
    session.commit()
    _log.info(f"Total DAG runs deleted: {deleted}")


def clean_dags(session, **context):
    from airflow.models import DagModel

    _log.info("Starting to run Clear Process")

    dags = session.query(DagModel).all()
    entries_to_delete = []

    _log.info(f"Found DAGs: {len(dags)}")

    for dag in dags:
        # Check if it is a zip-file
        if dag.fileloc is not None and ".zip/" in dag.fileloc:
            index = dag.fileloc.rfind(".zip/") + len(".zip")
            fileloc = dag.fileloc[0:index]
        else:
            fileloc = dag.fileloc

        if fileloc is None:
            _log.info(f"Adding to delete - `fileloc` None for DAG: {dag}")
            entries_to_delete.append(dag)
        elif not os.path.exists(fileloc):
            _log.info(f"Adding to delete - file does not exist for DAG: {dag}")
            entries_to_delete.append(dag)
        else:
            _log.info(f"Found valid file for DAG: {dag}")

    _log.info(f"Deleting dags:\n{len(entries_to_delete)}")

    for entry in entries_to_delete:
        session.delete(entry)

    _log.info("Committing DAG deletions")
    session.commit()
    _log.info(f"Total DAGs deleted: {len(entries_to_delete)}")


def _get_api_base_url(base_url=None):
    if base_url:
        return base_url.rstrip("/")
    if os.environ.get("AIRFLOW__API__BASE_URL"):
        return os.environ["AIRFLOW__API__BASE_URL"].rstrip("/")
    # Workers always know the execution API server, which shares a base with the public API
    if os.environ.get("AIRFLOW__CORE__EXECUTION_API_SERVER_URL"):
        return os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"].rstrip("/").removesuffix("/execution")
    try:
        from airflow.configuration import conf

        url = conf.get("api", "base_url", fallback=None)
        if url:
            return url.rstrip("/")
    except Exception as e:  # noqa: BLE001
        _log.debug(f"Could not read api base_url from airflow config: {e}")
    return "http://localhost:8080"


def _get_api_token(base_url, token=None, username=None, password=None):
    from ..balancer._pool_runtime import _http_request

    token = token or os.environ.get("AIRFLOW_API_TOKEN") or os.environ.get("AIRFLOW_CLI_TOKEN")
    if token:
        return token
    username = username or os.environ.get("AIRFLOW_API_USERNAME")
    password = password or os.environ.get("AIRFLOW_API_PASSWORD")
    try:
        if username and password:
            return _http_request(base_url, "POST", "/auth/token", body={"username": username, "password": password})["access_token"]
        # Simple auth manager with all-admins mode issues tokens without credentials
        return _http_request(base_url, "GET", "/auth/token")["access_token"]
    except RuntimeError as e:
        raise RuntimeError(
            f"Could not authenticate to the Airflow API at {base_url}: {e}. Set AIRFLOW_API_TOKEN, or AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD."
        ) from e


def _create_api_request(backend=None):
    """Build a request callable for the Airflow REST API, sharing the balancer pool runtime backends.

    Backends: "airflow3" resolves credentials from an Airflow connection (falling back to
    environment variables), "mwaa" uses boto3 invoke_rest_api with the worker's IAM role.
    "auto" picks mwaa when an MWAA environment is configured or detected.
    """
    from ..balancer import _pool_runtime

    backend = backend or {}
    kind = backend.get("kind", "auto")
    if kind == "auto":
        kind = "mwaa" if (backend.get("mwaa_environment_name") or os.environ.get("AIRFLOW_ENV_NAME")) else "airflow3"

    if kind == "mwaa":
        return _pool_runtime._mwaa_request(backend)

    try:
        return _pool_runtime._airflow3_request(backend)
    except Exception as e:  # noqa: BLE001
        _log.debug(f"Could not create API client from connection: {e}")

    base_url = _get_api_base_url(backend.get("base_url"))
    token = _get_api_token(base_url, token=backend.get("token"))

    def request(method, path, query=None, body=None, missing_ok=False):
        return _pool_runtime._http_request(base_url, method, f"/api/v2{path}", query=query, body=body, token=token, missing_ok=missing_ok)

    return request


def _api_get_paginated(request, path, key, **query):
    items = []
    offset = 0
    while True:
        response = request("GET", path, query={"limit": 100, "offset": offset, **query})
        batch = list(response.get(key) or [])
        items.extend(batch)
        total = response.get("total_entries")
        if not batch or (total is not None and len(items) >= total):
            return items
        offset += len(batch)


def _dag_run_date(run):
    value = run.get("logical_date") or run.get("run_after") or run.get("start_date")
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def clean_dag_runs_api(delete_successful, delete_failed, mark_failed_as_successful, max_dagruns, days_to_keep, backend=None):
    """REST API implementation of clean_dag_runs, for Airflow 3 where tasks have no database access."""
    request = _create_api_request(backend)

    utc_now = datetime.now(tz=UTC)
    cutoff_date = utc_now - timedelta(days=days_to_keep)
    _log.info(f"Cutoff date for clean: {cutoff_date}")

    dags = _api_get_paginated(request, "/dags", "dags", exclude_stale="false")
    dag_ids = [d["dag_id"] for d in dags]
    _log.info(f"Found DAGs to clean up: {dag_ids}")

    deleted = 0

    for dag_id in dag_ids:
        _log.info(f"Cleaning up DAG: {dag_id}")

        runs_path = f"/dags/{quote(dag_id, safe='')}/dagRuns"
        runs = _api_get_paginated(request, runs_path, "dag_runs")

        if delete_successful is False:
            _log.info(f"Not deleting successful DAG runs for DAG: {dag_id}")
            runs = [r for r in runs if r["state"] != "success"]
        if delete_failed is False:
            _log.info(f"Not deleting failed DAG runs for DAG: {dag_id}")
            runs = [r for r in runs if r["state"] != "failed"]

        runs.sort(key=lambda r: _dag_run_date(r) or utc_now)
        total_runs = len(runs)
        _log.info(f"Found {total_runs} DAG runs to clean up for DAG: {dag_id}")

        for run in runs:
            run_id = run["dag_run_id"]
            run_date = _dag_run_date(run)
            if (run_date is not None and run_date < cutoff_date) or total_runs > max_dagruns:
                if run["state"] not in ("queued", "success", "failed"):
                    # The API only allows deleting runs in queued/success/failed states
                    _log.info(f"Skipping DAG run in {run['state']} state: {run_id}")
                    continue
                _log.info(f"Deleting DAG run: {run_id}")
                try:
                    request("DELETE", f"{runs_path}/{quote(run_id, safe='')}")
                except RuntimeError as e:
                    _log.warning(f"Failed to delete DAG run {run_id}: {e}")
                    continue
                deleted += 1
                total_runs -= 1
            elif mark_failed_as_successful:
                # Need to iterate through all remaining
                if run["state"] == "failed":
                    _log.info(f"Marking failed DAG run as successful: {run_id}")
                    request("PATCH", f"{runs_path}/{quote(run_id, safe='')}", body={"state": "success"})
            else:
                break  # Since they are ordered, no more to delete

    _log.info(f"Total DAG runs deleted: {deleted}")


def clean_dags_api(backend=None):
    """REST API implementation of clean_dags, for Airflow 3 where tasks have no database access.

    Instead of checking file existence locally (meaningless on a worker), deletes DAGs the
    dag processor has marked stale (their file or bundle no longer exists).
    """
    request = _create_api_request(backend)

    _log.info("Starting to run Clear Process")

    dags = _api_get_paginated(request, "/dags", "dags", exclude_stale="false")
    _log.info(f"Found DAGs: {len(dags)}")

    stale_dag_ids = [d["dag_id"] for d in dags if d.get("is_stale")]
    _log.info(f"Deleting dags:\n{len(stale_dag_ids)}")

    for dag_id in stale_dag_ids:
        _log.info(f"Deleting stale DAG: {dag_id}")
        try:
            request("DELETE", f"/dags/{quote(dag_id, safe='')}")
        except RuntimeError as e:
            _log.warning(f"Failed to delete DAG {dag_id}: {e}")

    _log.info(f"Total DAGs deleted: {len(stale_dag_ids)}")
