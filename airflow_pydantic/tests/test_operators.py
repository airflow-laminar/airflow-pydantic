from airflow_pydantic import BashOperatorArgs, PythonOperatorArgs, SSHOperatorArgs


def test(**kwargs): ...


class TestOperators:
    def test_python_operator_args(self):
        PythonOperatorArgs(
            python_callable="airflow_pydantic.tests.test_operators.test",
            op_args=["test"],
            op_kwargs={"test": "test"},
            templates_dict={"test": "test"},
            templates_exts=[".sql", ".hql"],
            show_return_value_in_logs=True,
        )

    def test_bash_operator_args(self):
        BashOperatorArgs(
            bash_command="test",
            env={"test": "test"},
            append_env=True,
            output_encoding="utf-8",
            skip_exit_code=True,
            skip_on_exit_code=99,
            cwd="test",
            output_processor="airflow_pydantic.tests.test_operators.test",
        )

    def test_ssh_operator_args(self):
        SSHOperatorArgs(
            ssh_conn_id="test",
            command="test",
            do_xcom_push=True,
            timeout=10,
            get_pty=True,
            env={"test": "test"},
        )
