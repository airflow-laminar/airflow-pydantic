from datetime import datetime

from airflow_pydantic import Dag, DagArgs


class TestDag:
    def test_dag_args(self):
        d = DagArgs(
            description="",
            schedule="* * * * *",
            start_date=datetime.today(),
            end_date=datetime.today(),
            max_active_tasks=1,
            max_active_runs=1,
            default_view="grid",
            orientation="LR",
            catchup=False,
            is_paused_upon_creation=True,
            tags=["a", "b"],
            dag_display_name="test",
            enabled=True,
        )

        # Test roundtrips
        assert d == DagArgs.model_validate(d.model_dump())
        assert d == DagArgs.model_validate_json(d.model_dump_json())

    def test_dag(self):
        d = Dag(
            dag_id="a-dag",
            default_args=None,
            tasks={},
        )

        # Test roundtrips
        assert d == Dag.model_validate(d.model_dump())
        assert d == Dag.model_validate_json(d.model_dump_json())
