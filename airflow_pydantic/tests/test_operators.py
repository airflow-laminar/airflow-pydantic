from airflow_pydantic import BashCommands, BashOperatorArgs, PythonOperatorArgs, SSHOperatorArgs

from .conftest import hook


class TestOperators:
    def test_python_operator_args(self, python_operator_args):
        o = python_operator_args

        # Test roundtrips
        assert o == PythonOperatorArgs.model_validate(o.model_dump(exclude_unset=True))
        assert o == PythonOperatorArgs.model_validate_json(o.model_dump_json(exclude_unset=True))

    def test_bash_operator_args(self, bash_operator_args):
        o = bash_operator_args

        # Test roundtrips
        assert o == BashOperatorArgs.model_validate(o.model_dump(exclude_unset=True))
        assert o == BashOperatorArgs.model_validate_json(o.model_dump_json(exclude_unset=True))

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
        assert o.model_dump(exclude_unset=True) == SSHOperatorArgs.model_validate(o.model_dump(exclude_unset=True)).model_dump(exclude_unset=True)

        # NOTE: sshhook has no __eq__, so compare via json serialization
        assert o.model_dump_json(exclude_unset=True) == SSHOperatorArgs.model_validate_json(o.model_dump_json(exclude_unset=True)).model_dump_json(
            exclude_unset=True
        )

    def test_bash(self):
        cmds = BashCommands(
            commands=[
                "echo 'hello world'",
                "echo 'goodbye world'",
            ]
        )
        assert cmds.model_dump() == "bash -lc 'set -ex\necho 'hello world'\necho 'goodbye world''"
