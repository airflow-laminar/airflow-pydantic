from datetime import datetime, timedelta

from pytest import fixture

from airflow_pydantic import BashOperator, BashOperatorArgs, Dag, DagArgs, PythonOperator, PythonOperatorArgs, SSHOperator, SSHOperatorArgs, TaskArgs


def foo(**kwargs): ...


def hook(**kwargs):
    from airflow.providers.ssh.hooks.ssh import SSHHook

    return SSHHook(remote_host="test", username="test")


@fixture
def python_operator_args():
    return PythonOperatorArgs(
        python_callable="airflow_pydantic.tests.conftest.foo",
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
        output_processor="airflow_pydantic.tests.conftest.foo",
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
        ssh_hook="airflow_pydantic.tests.conftest.hook",
        command="test",
        do_xcom_push=True,
        cmd_timeout=10,
        get_pty=True,
        environment={"test": "test"},
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


@fixture
def dag(dag_args, task_args, python_operator, bash_operator, ssh_operator):
    return Dag(
        dag_id="a-dag",
        **dag_args.model_dump(),
        default_args=task_args,
        tasks={
            "task1": python_operator,
            "task2": bash_operator,
            "task3": ssh_operator,
        },
    )


@fixture
def dag_with_external(dag_args, task_args, python_operator, bash_operator, ssh_operator):
    ssh_operator.ssh_hook = hook
    return Dag(
        dag_id="a-dag",
        **dag_args.model_dump(),
        default_args=task_args,
        tasks={
            "task1": python_operator,
            "task2": bash_operator,
            "task3": ssh_operator,
        },
    )
