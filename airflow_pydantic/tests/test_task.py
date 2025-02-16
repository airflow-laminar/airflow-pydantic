from datetime import datetime, timedelta

from airflow_pydantic import Task, TaskArgs


class TestTask:
    def test_task_args(self):
        TaskArgs(
            owner="airflow",
            email=["test@test.com"],
            email_on_failure=True,
            email_on_retry=True,
            retries=3,
            retry_delay=timedelta(minutes=5),
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 1, 2),
            depends_on_past=True,
            queue="default",
            pool="default",
            pool_slots=1,
            do_xcom_push=True,
            task_display_name="test",
        )

    def test_task(self):
        Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=[],
            args=None,
        )
