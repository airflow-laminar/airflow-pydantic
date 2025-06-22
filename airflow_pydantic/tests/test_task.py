from airflow_pydantic import Task, TaskArgs


class TestTask:
    def test_task_args(self, task_args):
        t = task_args

        # Test roundtrips
        assert t == TaskArgs.model_validate(t.model_dump(exclude_unset=True))
        assert t == TaskArgs.model_validate_json(t.model_dump_json(exclude_unset=True))

    def test_task(self):
        t = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=[],
            args=None,
        )

        # Test roundtrips
        assert t == Task.model_validate(t.model_dump(exclude_unset=True))
        assert t == Task.model_validate_json(t.model_dump_json(exclude_unset=True))

    def test_operators(self):
        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t2 = Task(
            task_id="b-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t3 = Task(
            task_id="c-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t4 = Task(
            task_id="d-task",
            operator="airflow.operators.empty.EmptyOperator",
        )

        # T1 -> T2
        t1 >> t2
        # T2 -> T3
        t3 << t2
        # T2 -> T4
        # T3 -> T4
        t4 << [t2, t3]
        # T1 -> T3
        # T1 -> T4
        t1 >> [t3, t4]

        assert t1.dependencies is None
        assert t2.dependencies == [t1.task_id]
        assert t3.dependencies == [t2.task_id, t1.task_id]
        assert t4.dependencies == [t2.task_id, t3.task_id, t1.task_id]

    def test_setters(self):
        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t2 = Task(
            task_id="b-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t3 = Task(
            task_id="c-task",
            operator="airflow.operators.empty.EmptyOperator",
        )
        t4 = Task(
            task_id="d-task",
            operator="airflow.operators.empty.EmptyOperator",
        )

        # T1 -> T2
        t1.set_downstream(t2)
        # T2 -> T3
        t3.set_upstream(t2)
        # T2 -> T4
        # T3 -> T4
        t4.set_upstream([t2, t3])
        # T1 -> T3
        # T1 -> T4
        t1.set_downstream([t3, t4])

        assert t1.dependencies is None
        assert t2.dependencies == [t1.task_id]
        assert t3.dependencies == [t2.task_id, t1.task_id]
        assert t4.dependencies == [t2.task_id, t3.task_id, t1.task_id]

    def test_task_dependency_normalization(self):
        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies="a",
        )
        assert t1.dependencies == ["a"]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=["a"],
        )
        assert t1.dependencies == ["a"]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies="a.b",
        )
        assert t1.dependencies == [("a", "b")]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=["a.b", "a.b.c"],
        )
        assert t1.dependencies == [("a", "b"), ("a.b", "c")]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=t1,
        )
        assert t1.dependencies == ["a-task"]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=[t1],
        )
        assert t1.dependencies == ["a-task"]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=[(t1, "c")],
        )
        assert t1.dependencies == [("a-task", "c")]

        t1 = Task(
            task_id="a-task",
            operator="airflow.operators.empty.EmptyOperator",
            dependencies=["a", "a.b.c", t1, ("a.b", "c"), (t1, "c")],
        )
        assert t1.dependencies == ["a", ("a.b", "c"), "a-task", ("a.b", "c"), ("a-task", "c")]
