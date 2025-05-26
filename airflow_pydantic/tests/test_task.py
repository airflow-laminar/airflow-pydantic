from datetime import datetime, timedelta

from airflow_pydantic import Task, TaskArgs


class TestTask:
    def test_task_args(self):
        t = TaskArgs(
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

        # Test roundtrips
        assert t == TaskArgs.model_validate(t.model_dump())
        assert t == TaskArgs.model_validate_json(t.model_dump_json())

    def test_task(self):
        t = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=[],
            args=None,
        )

        # Test roundtrips
        assert t == Task.model_validate(t.model_dump())
        assert t == Task.model_validate_json(t.model_dump_json())
