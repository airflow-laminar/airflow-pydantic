import ast

import pytest
from dateutil.relativedelta import MO, relativedelta
from pydantic import TypeAdapter

from airflow_pydantic import ExternalTaskMarker, HITLEntryTask, Pool
from airflow_pydantic.core.render.utils import _get_parts_from_value
from airflow_pydantic.utils.relativedelta import RelativeDelta, Weekday


def test_pool_render_imports_are_flat():
    imports, expression = _get_parts_from_value("custom", Pool(pool="configured"))
    assert all(isinstance(item, ast.ImportFrom) for item in imports)
    assert ast.unparse(expression) == "'configured'"


def test_weekday_adapter_roundtrip():
    adapter = TypeAdapter(Weekday)
    value = adapter.validate_python({"weekday": 0, "n": 1})
    assert value == MO(1)
    assert adapter.validate_json(adapter.dump_json(value)) == value


def test_relativedelta_adapter_roundtrip():
    adapter = TypeAdapter(RelativeDelta)
    value = relativedelta(days=2, hours=3)
    assert adapter.validate_json(adapter.dump_json(value)) == value


@pytest.mark.parametrize("task_type,operator", [(ExternalTaskMarker, "ExternalTaskMarker"), (HITLEntryTask, "HITLEntryOperator")])
def test_marker_operators_render(task_type, operator):
    imports, _, rendered = task_type(task_id="marker").render()
    assert any(operator in item for item in imports)
    assert operator in rendered
