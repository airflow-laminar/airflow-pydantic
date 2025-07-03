from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from pytz import UTC

from ...airflow import AirflowFailException, AirflowSkipException

if TYPE_CHECKING:
    pass

__all__ = (
    "skip",
    "fail",
    "pass_",
    "cleanup_dag_runs",
)


def skip():
    raise AirflowSkipException


def fail():
    raise AirflowFailException


def pass_():
    pass


def cleanup_dag_runs(session, delete_successful, delete_failed, mark_failed_as_successful, max_dagruns, days_to_keep):
    from airflow.models import DagModel, DagRun
    from airflow.utils.state import State

    # Make cutoff_date timezone-aware (UTC)
    utc_now = datetime.utcnow().replace(tzinfo=UTC)
    cutoff_date = utc_now - timedelta(days=days_to_keep)

    # Fetch all DAGs from the DagBag
    dag_ids = [d.dag_id for d in session.query(DagModel.dag_id).distinct(DagModel.dag_id).all()]

    deleted = 0

    for dag_id in dag_ids:
        print(f"Cleaning up DAG: {dag_id}")

        # Query for DAG runs of each DAG
        query = session.query(DagRun).filter(DagRun.dag_id == dag_id)

        if delete_successful is False:
            query = query.filter(DagRun.state != State.SUCCESS)
        if delete_failed is False:
            query = query.filter(DagRun.state != State.FAILED)

        dagruns = query.order_by(DagRun.execution_date.asc()).all()
        total_runs = len(dagruns)

        for dr in dagruns:
            # Compare execution_date (offset-aware) with cutoff_date (now offset-aware)
            if dr.execution_date < cutoff_date or total_runs > max_dagruns:
                session.delete(dr)
                deleted += 1
                total_runs -= 1  # Adjust count since we deleted one
            elif mark_failed_as_successful:
                # Need to iterate through all remaining
                if dr.state == State.FAILED:
                    # Mark failed runs as successful
                    dr.state = State.SUCCESS
                    session.merge(dr)
            elif not mark_failed_as_successful:
                break  # Since they are ordered, no more to delete

    session.commit()
    print(f"Total DAG runs deleted: {deleted}")
