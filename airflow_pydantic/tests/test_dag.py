from importlib.metadata import version

if version("apache-airflow") >= "3.0.0":
    _AIRFLOW_3 = True
else:
    _AIRFLOW_3 = False

from airflow_pydantic import Dag, DagArgs


class TestDag:
    def test_dag_args(self, dag_args):
        d = dag_args
        # Test roundtrips
        assert d == DagArgs.model_validate(d.model_dump(exclude_unset=True))
        assert d == DagArgs.model_validate_json(d.model_dump_json(exclude_unset=True))

    def test_dag(self):
        d = Dag(
            dag_id="a-dag",
            default_args=None,
            tasks={},
        )

        # Test roundtrips
        assert d == Dag.model_validate(d.model_dump(exclude_unset=True))
        assert d == Dag.model_validate_json(d.model_dump_json(exclude_unset=True))

    def test_dag_none_schedule(self, dag_none_schedule):
        d = dag_none_schedule
        # Test roundtrips
        assert d == Dag.model_validate(d.model_dump(exclude_unset=True))
        assert d == Dag.model_validate_json(d.model_dump_json(exclude_unset=True))

        inst = d.instantiate()
        assert inst.dag_id == "a-dag"

        if _AIRFLOW_3:
            assert inst.schedule is None
        else:
            assert inst.schedule_interval is None
