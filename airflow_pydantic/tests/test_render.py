class TestRender:
    def test_render_operator(self, python_operator):
        imports, globals_, task = python_operator.render()
        assert imports == [
            "from airflow.operators.python import PythonOperator",
            "from airflow_pydantic.tests.test_operators import test",
        ]
        assert globals_ == []
        assert task == [
            "PythonOperator(python_callable=test, op_args=['test'], op_kwargs={'test': 'test'}, templates_dict={'test': 'test'}, templates_exts=['.sql', '.hql'], show_return_value_in_logs=True, task_id='test_python_operator')"
        ]

    def test_render_operator_dag_from_context(self, python_operator):
        imports, globals_, task = python_operator.render(dag_from_context=True)
        assert imports == [
            "from airflow.operators.python import PythonOperator",
            "from airflow_pydantic.tests.test_operators import test",
        ]
        assert globals_ == []
        assert task == [
            "PythonOperator(python_callable=test, op_args=['test'], op_kwargs={'test': 'test'}, templates_dict={'test': 'test'}, templates_exts=['.sql', '.hql'], show_return_value_in_logs=True, task_id='test_python_operator', dag=dag)"
        ]

    def test_render_operator_bash(self, bash_operator):
        imports, globals_, task = bash_operator.render()
        assert imports == [
            "from airflow.operators.bash import BashOperator",
            "from airflow_pydantic.tests.test_operators import test",
        ]
        assert globals_ == []
        assert task == [
            "BashOperator(bash_command='test', env={'test': 'test'}, append_env=True, output_encoding='utf-8', skip_exit_code=True, skip_on_exit_code=99, cwd='test', output_processor=test, task_id='test_bash_operator')",
        ]

    def test_render_operator_ssh(self, ssh_operator):
        imports, globals_, task = ssh_operator.render()
        assert imports == [
            "from airflow.providers.ssh.operators.ssh import SSHOperator",
        ]
        assert globals_ == []
        assert task == [
            "SSHOperator(do_xcom_push=True, ssh_conn_id='test', command='test', get_pty=True, task_id='test_ssh_operator', timeout=10, env={'test': 'test'})",
        ]

    # def test_render(self):
    #     dag = Dag(
    #         description="",
    #         schedule="* * * * *",
    #         start_date=datetime.today(),
    #         end_date=datetime.today(),
    #         max_active_tasks=1,
    #         max_active_runs=1,
    #         default_view="grid",
    #         orientation="LR",
    #         catchup=False,
    #         is_paused_upon_creation=True,
    #         tags=["a", "b"],
    #         dag_display_name="test",
    #         enabled=True,
    #         dag_id="a-dag",
    #         default_args=None,
    #         tasks={
    #             "task1": PythonOperator(
    #                 python_callable="airflow_pydantic.tests.test_operators.test",
    #                 op_args=["test"],
    #                 op_kwargs={"test": "test"},
    #                 templates_dict={"test": "test"},
    #                 templates_exts=[".sql", ".hql"],
    #                 show_return_value_in_logs=True,
    #             ),
    #             "task2": BashOperator(
    #                 bash_command="test",
    #                 env={"test": "test"},
    #                 append_env=True,
    #                 output_encoding="utf-8",
    #                 skip_exit_code=True,
    #                 skip_on_exit_code=99,
    #                 cwd="test",
    #                 output_processor="airflow_pydantic.tests.test_operators.test",
    #             ),
    #             "task3": SSHOperator(
    #                 ssh_conn_id="test",
    #                 command="test",
    #                 do_xcom_push=True,
    #                 timeout=10,
    #                 get_pty=True,
    #                 env={"test": "test"},
    #             ),
    #         },
    #     )
    #     assert dag.render() == ""
