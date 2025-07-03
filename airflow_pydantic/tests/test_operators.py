import pytest

from airflow_pydantic import BashCommands, BashOperatorArgs, BashSensorArgs, PythonOperatorArgs, SSHOperatorArgs
from airflow_pydantic.migration import _airflow_3


class TestOperators:
    def test_python_operator_args(self, python_operator_args):
        o = python_operator_args

        # Test roundtrips
        assert o == PythonOperatorArgs.model_validate(o.model_dump(exclude_unset=True))
        assert o == PythonOperatorArgs.model_validate_json(o.model_dump_json(exclude_unset=True))

    def test_python_operator(self, python_operator):
        if _airflow_3() is None:
            return pytest.skip("Airflow not installed")
        python_operator.instantiate()

    def test_bash_operator_args(self, bash_operator_args):
        o = bash_operator_args

        # Test roundtrips
        assert o == BashOperatorArgs.model_validate(o.model_dump(exclude_unset=True))

        jsn = o.model_dump_json(exclude_unset=True)
        obj = BashOperatorArgs.model_validate_json(jsn)

        # pool has no __eq__
        obj.pool = o.pool
        assert o == obj

    def test_bash_operator(self, bash_operator):
        if _airflow_3() is None:
            return pytest.skip("Airflow not installed")
        bash_operator.instantiate()

    def test_ssh_operator_args(self, ssh_operator_args):
        o = ssh_operator_args

        # Test roundtrips
        assert o.model_dump(exclude_unset=True) == SSHOperatorArgs.model_validate(o.model_dump(exclude_unset=True)).model_dump(exclude_unset=True)

        # NOTE: sshhook has no __eq__, so compare via json serialization
        assert o.model_dump_json(exclude_unset=True) == SSHOperatorArgs.model_validate_json(o.model_dump_json(exclude_unset=True)).model_dump_json(
            exclude_unset=True
        )

    def test_ssh_operator(self, ssh_operator):
        if _airflow_3() is None:
            return pytest.skip("Airflow not installed")
        ssh_operator.instantiate()

    def test_bash_sensor_args(self, bash_sensor_args):
        o = bash_sensor_args

        # Test roundtrips
        assert o == BashSensorArgs.model_validate(o.model_dump(exclude_unset=True))
        assert o == BashSensorArgs.model_validate_json(o.model_dump_json(exclude_unset=True))

    def test_bash_sensor(self, bash_sensor):
        if _airflow_3() is None:
            return pytest.skip("Airflow not installed")
        bash_sensor.instantiate()

    def test_bash(self):
        cmds = BashCommands(
            commands=[
                "echo 'hello world'",
                "echo 'goodbye world'",
            ]
        )
        assert cmds.model_dump() == "bash -lc 'set -ex\necho 'hello world'\necho 'goodbye world''"
