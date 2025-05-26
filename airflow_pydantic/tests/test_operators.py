from airflow_pydantic import BashOperatorArgs, PythonOperatorArgs, SSHOperatorArgs


def test(**kwargs): ...


def test_hook(**kwargs):
    from airflow.providers.ssh.hooks.ssh import SSHHook

    return SSHHook(remote_host="test")


class TestOperators:
    def test_python_operator_args(self):
        o = PythonOperatorArgs(
            python_callable="airflow_pydantic.tests.test_operators.test",
            op_args=["test"],
            op_kwargs={"test": "test"},
            templates_dict={"test": "test"},
            templates_exts=[".sql", ".hql"],
            show_return_value_in_logs=True,
        )

        # Test roundtrips
        assert o == PythonOperatorArgs.model_validate(o.model_dump())
        assert o == PythonOperatorArgs.model_validate_json(o.model_dump_json())

    def test_bash_operator_args(self):
        o = BashOperatorArgs(
            bash_command="test",
            env={"test": "test"},
            append_env=True,
            output_encoding="utf-8",
            skip_exit_code=True,
            skip_on_exit_code=99,
            cwd="test",
            output_processor="airflow_pydantic.tests.test_operators.test",
        )

        # Test roundtrips
        assert o == BashOperatorArgs.model_validate(o.model_dump())
        assert o == BashOperatorArgs.model_validate_json(o.model_dump_json())

    def test_ssh_operator_args(self):
        o = SSHOperatorArgs(
            ssh_hook=test_hook(),
            ssh_conn_id="test",
            command="test",
            do_xcom_push=True,
            timeout=10,
            get_pty=True,
            env={"test": "test"},
        )

        o = SSHOperatorArgs(
            ssh_hook="airflow_pydantic.tests.test_operators.test_hook",
            ssh_conn_id="test",
            command="test",
            do_xcom_push=True,
            timeout=10,
            get_pty=True,
            env={"test": "test"},
        )

        # Test roundtrips
        assert o == SSHOperatorArgs.model_validate(o.model_dump())

        # NOTE: sshhook has no __eq__, so compare via json serialization
        assert o.model_dump_json() == SSHOperatorArgs.model_validate_json(o.model_dump_json()).model_dump_json()
