from datetime import datetime, timedelta

from pytest import fixture

from airflow_pydantic import BashOperator, BashOperatorArgs, DagArgs, PythonOperator, PythonOperatorArgs, SSHOperator, SSHOperatorArgs, TaskArgs


@fixture
def python_operator_args():
    return PythonOperatorArgs(
        python_callable="airflow_pydantic.tests.test_operators.test",
        op_args=["test"],
        op_kwargs={"test": "test"},
        templates_dict={"test": "test"},
        templates_exts=[".sql", ".hql"],
        show_return_value_in_logs=True,
    )


@fixture
def python_operator(python_operator_args):
    return PythonOperator(
        task_id="test_python_operator",
        **python_operator_args.model_dump(),
    )


@fixture
def bash_operator_args():
    return BashOperatorArgs(
        bash_command="test",
        env={"test": "test"},
        append_env=True,
        output_encoding="utf-8",
        skip_exit_code=True,
        skip_on_exit_code=99,
        cwd="test",
        output_processor="airflow_pydantic.tests.test_operators.test",
    )


@fixture
def bash_operator(bash_operator_args):
    return BashOperator(
        task_id="test_bash_operator",
        **bash_operator_args.model_dump(),
    )


@fixture
def ssh_operator_args():
    return SSHOperatorArgs(
        ssh_conn_id="test",
        command="test",
        do_xcom_push=True,
        timeout=10,
        get_pty=True,
        env={"test": "test"},
    )


@fixture
def ssh_operator(ssh_operator_args):
    return SSHOperator(
        task_id="test_ssh_operator",
        **ssh_operator_args.model_dump(),
    )


@fixture
def dag_args():
    return DagArgs(
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


@fixture
def task_args():
    return TaskArgs(
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
