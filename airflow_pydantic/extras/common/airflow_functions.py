import os
from datetime import datetime, timedelta
from logging import getLogger

from pytz import UTC

from ...airflow import AirflowFailException, AirflowSkipException
from ...migration import _airflow_3

__all__ = (
    "clean_dag_runs",
    "clean_dags",
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


if _airflow_3():

    def _list_all_dags(client):
        """Paginate through all DAGs."""
        dags = []
        offset = 0
        while True:
            resp = client.dags.list(limit=100, offset=offset)
            dags.extend(resp.dags)
            if offset + 100 >= resp.total_entries:
                break
            offset += 100
        return dags

    def _list_all_dag_runs(client, dag_id):
        """Paginate through all DAG runs for a given DAG."""
        runs = []
        offset = 0
        while True:
            resp = client.dag_runs.list(dag_id, limit=100, offset=offset)
            runs.extend(resp.dag_runs)
            if offset + 100 >= resp.total_entries:
                break
            offset += 100
        return runs

    def clean_dag_runs(delete_successful, delete_failed, mark_failed_as_successful, max_dagruns, days_to_keep, **kwargs):
        from airflow.api.client import get_local_rest_client

        client = get_local_rest_client(process_type="cleanup")

        utc_now = datetime.now(tz=UTC)
        cutoff_date = utc_now - timedelta(days=days_to_keep)
        _log.info(f"Cutoff date for clean: {cutoff_date}")

        all_dags = _list_all_dags(client)
        dag_ids = [d.dag_id for d in all_dags]
        _log.info(f"Found DAGs to clean up: {dag_ids}")

        deleted = 0

        for dag_id in dag_ids:
            _log.info(f"Cleaning up DAG: {dag_id}")

            runs = _list_all_dag_runs(client, dag_id)

            # Filter by state
            if delete_successful is False:
                _log.info(f"Not deleting successful DAG runs for DAG: {dag_id}")
                runs = [r for r in runs if str(r.state) != "success"]
            if delete_failed is False:
                _log.info(f"Not deleting failed DAG runs for DAG: {dag_id}")
                runs = [r for r in runs if str(r.state) != "failed"]

            # Sort by logical_date ascending (use datetime.min as fallback for None)
            _min_dt = datetime.min.replace(tzinfo=UTC)
            runs.sort(key=lambda r: r.logical_date or _min_dt)

            total_runs = len(runs)
            _log.info(f"Found {total_runs} DAG runs to clean up for DAG: {dag_id}")

            for dr in runs:
                logical_date = dr.logical_date
                if (logical_date and logical_date < cutoff_date) or total_runs > max_dagruns:
                    _log.info(f"Deleting DAG run: {dr.dag_run_id}")
                    client.dag_runs.delete(dag_id, dr.dag_run_id)
                    deleted += 1
                    total_runs -= 1
                elif mark_failed_as_successful:
                    if str(dr.state) == "failed":
                        _log.info(f"Marking failed DAG run as successful: {dr.dag_run_id}")
                        client.dag_runs.update(dag_id, dr.dag_run_id, state="success")
                elif not mark_failed_as_successful:
                    break

        _log.info(f"Total DAG runs deleted: {deleted}")

    def clean_dags(**kwargs):
        from airflow.api.client import get_local_rest_client

        client = get_local_rest_client(process_type="cleanup")

        _log.info("Starting to run Clear Process")

        all_dags = _list_all_dags(client)
        entries_to_delete = []

        _log.info(f"Found DAGs: {len(all_dags)}")

        for dag in all_dags:
            fileloc = dag.fileloc

            # Check if it is a zip-file
            if fileloc is not None and ".zip/" in fileloc:
                index = fileloc.rfind(".zip/") + len(".zip")
                fileloc = fileloc[0:index]

            if fileloc is None:
                _log.info(f"Adding to delete - `fileloc` None for DAG: {dag.dag_id}")
                entries_to_delete.append(dag.dag_id)
            elif not os.path.exists(fileloc):
                _log.info(f"Adding to delete - file does not exist for DAG: {dag.dag_id}")
                entries_to_delete.append(dag.dag_id)
            else:
                _log.info(f"Found valid file for DAG: {dag.dag_id}")

        _log.info(f"Deleting dags:\n{len(entries_to_delete)}")

        for dag_id in entries_to_delete:
            client.dags.delete(dag_id)

        _log.info(f"Total DAGs deleted: {len(entries_to_delete)}")

else:

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
