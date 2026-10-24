import ast
import json
from copy import deepcopy

import pytest
from pydantic import ValidationError

from airflow_pydantic import Dag, DagArgs
from airflow_pydantic.migration import _airflow_3


@pytest.mark.parametrize("model_cls", [DagArgs, Dag])
@pytest.mark.parametrize(
    "tags, expected",
    [
        (None, None),
        ([], []),
        (["analytics", "report"], ["analytics", "report"]),
        ([["analytics", "daily"], "report"], ["analytics", "daily", "report"]),
        ([["analytics", ["daily"]], "report"], ["analytics", "daily", "report"]),
        ([[], [[], "report"], []], ["report"]),
        ([["same"], "same"], ["same", "same"]),
        (("analytics", ["report"]), ["analytics", "report"]),
    ],
)
def test_tag_lists(model_cls, tags, expected):
    original = deepcopy(tags)
    model = model_cls.model_validate({"tags": tags})
    assert model.tags == expected
    assert tags == original
    assert model_cls.model_validate_json(json.dumps({"tags": tags})).tags == expected
    assert model.model_dump(exclude_unset=True)["tags"] == expected
    assert model_cls.model_validate_json(model.model_dump_json(exclude_unset=True)).tags == expected


@pytest.mark.parametrize("model_cls", [DagArgs, Dag])
@pytest.mark.parametrize("tags", ["analytics", 1, {"analytics": "report"}, [[None]], [[True]], [[{"tag": "analytics"}]]])
def test_invalid_tags_remain_invalid(model_cls, tags):
    with pytest.raises(ValidationError):
        model_cls.model_validate({"tags": tags})
    with pytest.raises(ValidationError):
        model_cls.model_validate_json(json.dumps({"tags": tags}))


@pytest.mark.parametrize("model_cls", [DagArgs, Dag])
def test_nested_numbers_follow_model_string_validation(model_cls):
    if model_cls.model_config.get("coerce_numbers_to_str"):
        assert model_cls.model_validate({"tags": [[1]]}).tags == ["1"]
    else:
        with pytest.raises(ValidationError):
            model_cls.model_validate({"tags": [[1]]})


@pytest.mark.parametrize("model_cls", [DagArgs, Dag])
def test_bytes_tags_remain_whole(model_cls):
    assert model_cls.model_validate({"tags": [b"analytics", bytearray(b"report")]}).tags == ["analytics", "report"]


@pytest.mark.parametrize("model_cls", [DagArgs, Dag])
def test_tag_assignment(model_cls):
    model = model_cls(tags=["old"])
    tags = [["analytics"], "report"]
    model.tags = tags
    assert model.tags == ["analytics", "report"]
    assert tags == [["analytics"], "report"]
    model.tags = None
    assert model.tags is None


@pytest.mark.parametrize("convert", ["none", "all", "object"])
def test_hydra_interpolated_tag_lists(convert):
    hydra = pytest.importorskip("hydra.utils")
    omegaconf = pytest.importorskip("omegaconf")
    config = omegaconf.OmegaConf.create(
        {
            "common_tags": ["analytics", "daily"],
            "workflow_tags": ["${common_tags}", "etl"],
            "dag": {
                "_target_": "airflow_pydantic.Dag",
                "dag_id": "report",
                "tags": ["${workflow_tags}", "${dag.dag_id}"],
            },
        }
    )
    original = omegaconf.OmegaConf.to_container(config, resolve=True)
    model = hydra.instantiate(config.dag, _convert_=convert)
    assert model.tags == ["analytics", "daily", "etl", "report"]
    assert omegaconf.OmegaConf.to_container(config, resolve=True) == original


@pytest.mark.parametrize("airflow_major_version", [2, 3])
def test_render_flat_tags(airflow_major_version):
    model = Dag.model_validate({"dag_id": "report", "tags": [["analytics", "daily"], "report"]})
    tree = ast.parse(model.render(airflow_major_version=airflow_major_version))
    dag_call = next(node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "DAG")
    tags = next(keyword.value for keyword in dag_call.keywords if keyword.arg == "tags")
    assert ast.literal_eval(tags) == ["analytics", "daily", "report"]
    assert json.loads(model.model_dump_json(exclude_unset=True))["tags"] == ["analytics", "daily", "report"]


@pytest.mark.skipif(_airflow_3() is None, reason="Airflow is not installed")
def test_instantiate_flat_tags():
    model = Dag.model_validate({"dag_id": "report", "schedule": None, "tags": [["analytics", "daily"], "report"]})
    assert set(model.instantiate().tags) == {"analytics", "daily", "report"}
