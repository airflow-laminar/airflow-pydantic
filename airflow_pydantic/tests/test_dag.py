from airflow_pydantic import Dag, DagArgs


class TestDag:
    def test_dag_args(self, dag_args):
        d = dag_args
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
