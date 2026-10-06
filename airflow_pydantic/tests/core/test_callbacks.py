import ast
import json

import pytest

from airflow_pydantic import BashTask, TaskArgs
from airflow_pydantic.airflow import BashOperator as AirflowBashOperator, _AirflowPydanticMarker


def alert(context):
    return context


CALLBACK_FIELDS = ["on_failure_callback", "on_execute_callback", "on_retry_callback", "on_success_callback", "on_skipped_callback"]


@pytest.mark.parametrize("field", CALLBACK_FIELDS)
@pytest.mark.parametrize("as_list", [False, True])
def test_callback_paths_roundtrip(field, as_list):
    callback = [alert] if as_list else alert
    task = BashTask(task_id="callback-task", bash_command="true", **{field: callback})
    dumped = json.loads(task.model_dump_json(exclude_unset=True))
    assert dumped[field] == [f"{__name__}.alert"] if as_list else dumped[field] == f"{__name__}.alert"
    restored = BashTask.model_validate(dumped)
    assert getattr(restored, field) == callback


@pytest.mark.skipif(issubclass(AirflowBashOperator, _AirflowPydanticMarker), reason="Airflow is not installed")
@pytest.mark.parametrize("field", CALLBACK_FIELDS)
@pytest.mark.parametrize("as_list", [False, True])
def test_callback_instantiates_callable(field, as_list):
    callback = [alert] if as_list else alert
    task = BashTask(task_id="callback-task", bash_command="true", **{field: callback})
    assert getattr(task.instantiate(), field) in (callback, [callback])


@pytest.mark.parametrize("field", CALLBACK_FIELDS)
@pytest.mark.parametrize("as_list", [False, True])
def test_callback_render_imports_callable(field, as_list):
    task = BashTask(task_id="callback-task", bash_command="true", **{field: [alert] if as_list else alert})
    imports, _, rendered = task.render()
    assert f"from {__name__} import alert" in imports
    expression = ast.parse(rendered).body[0].value
    callback = next(keyword.value for keyword in expression.keywords if keyword.arg == field)
    if as_list:
        assert isinstance(callback, ast.List)
        callback = callback.elts[0]
    assert isinstance(callback, ast.Name)
    assert callback.id == "alert"


def test_callback_default_args_render():
    from airflow_pydantic.core.render.task import render_base_task_args

    imports, _, rendered = render_base_task_args(TaskArgs(on_failure_callback=[alert]))
    assert f"from {__name__} import alert" in imports
    assert "'on_failure_callback': [alert]" in rendered


def alert_again(context):
    return context


def test_multiple_callbacks_roundtrip_and_render_in_order():
    callbacks = [alert, alert_again]
    task = BashTask(task_id="callback-task", bash_command="true", on_failure_callback=callbacks)
    restored = BashTask.model_validate_json(task.model_dump_json(exclude_unset=True))
    assert restored.on_failure_callback == callbacks
    imports, _, rendered = restored.render()
    assert f"from {__name__} import alert" in imports
    assert f"from {__name__} import alert_again" in imports
    assert "on_failure_callback=[alert, alert_again]" in rendered
