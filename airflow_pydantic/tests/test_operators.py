from airflow_pydantic import BashOperatorArgs, PythonOperatorArgs, SSHOperatorArgs

from .conftest import hook


class TestOperators:
    def test_python_operator_args(self, python_operator_args):
        o = python_operator_args

        # Test roundtrips
        assert o == PythonOperatorArgs.model_validate(o.model_dump())
        assert o == PythonOperatorArgs.model_validate_json(o.model_dump_json())

    def test_bash_operator_args(self, bash_operator_args):
        o = bash_operator_args

        # Test roundtrips
        assert o == BashOperatorArgs.model_validate(o.model_dump())
        assert o == BashOperatorArgs.model_validate_json(o.model_dump_json())

    def test_ssh_operator_args(self, ssh_operator_args):
        o = SSHOperatorArgs(
            ssh_hook=hook(),
            ssh_conn_id="test",
            command="test",
            do_xcom_push=True,
            timeout=10,
            get_pty=True,
            env={"test": "test"},
        )

        o = ssh_operator_args

        # Test roundtrips
        assert o.model_dump() == SSHOperatorArgs.model_validate(o.model_dump()).model_dump()

        # NOTE: sshhook has no __eq__, so compare via json serialization
        assert o.model_dump_json() == SSHOperatorArgs.model_validate_json(o.model_dump_json()).model_dump_json()
