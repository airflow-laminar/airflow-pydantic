import ast
from importlib.util import find_spec
from unittest.mock import patch

import pytest

from airflow_pydantic import BalancerHostQueryConfiguration, SSHTask, SSHTaskArgs
from airflow_pydantic.testing import pools, variables


@pytest.mark.parametrize("airflow_major_version", [2, 3])
@pytest.mark.parametrize("raw", [False, True])
def test_host_filter_renders_hostname_template(ssh_operator_balancer_filter, airflow_major_version, raw):
    _, _, source = ssh_operator_balancer_filter.render(raw=raw, airflow_major_version=airflow_major_version)
    call = source if raw else ast.parse(source, mode="eval").body
    assert isinstance(call, ast.Call)
    options = {option.arg: ast.literal_eval(option.value) for option in call.func.value.keywords if option.arg == "map_index_template"}
    assert options == {"map_index_template": "{{ task.remote_host }}"}


@pytest.mark.parametrize("construction", ["instantiate", "render"])
def test_each_expanded_instance_renders_its_own_hostname(ssh_operator_balancer_filter, construction):
    airflow = pytest.importorskip("airflow")
    from airflow_pydantic.airflow import DAG

    with pools(), variables({"password": "password"}), DAG(dag_id="host-labels", schedule=None) as dag:
        if construction == "instantiate":
            mapped = ssh_operator_balancer_filter.instantiate(dag=dag)
        else:
            imports, globals_, source = ssh_operator_balancer_filter.render(airflow_major_version=int(airflow.__version__.split(".")[0]))
            namespace = {}
            exec("\n".join([*imports, *globals_, f"task = {source}"]), namespace)
            mapped = namespace["task"]
        assert mapped.map_index_template == "{{ task.remote_host }}"
        for host in ["test_host_1.local", "test_host_2.local"]:
            task = mapped.unmap({"remote_host": host})
            assert task.render_template(task.map_index_template, {"task": task}) == host


@pytest.mark.parametrize("label", ["{{ task.remote_host | upper }}", None, ""])
def test_explicit_template_overrides_survive_task_templates(balancer_multi, label):
    with pools(), variables({"password": "password"}):
        template = SSHTaskArgs(
            command="hostname",
            map_index_template=label,
            ssh_hook=BalancerHostQueryConfiguration(kind="filter", balancer=balancer_multi, tag="worker"),
        )
    task = SSHTask.model_validate({"task_id": "custom-label", "template": template})
    assert task.map_index_template == label
    assert task.model_dump(exclude_unset=True)["map_index_template"] == label
    _, _, source = task.render(raw=True)
    options = {option.arg: ast.literal_eval(option.value) for option in source.func.value.keywords if option.arg == "map_index_template"}
    assert options == {"map_index_template": label}
    if find_spec("airflow") is not None:
        with pools(), variables({"password": "password"}):
            assert task.instantiate().map_index_template == label


def test_single_host_keeps_unnamed_default(ssh_operator):
    assert ssh_operator.map_index_template is None
    assert "map_index_template" not in ssh_operator.model_dump(exclude_unset=True)
    assert "map_index_template" not in ssh_operator.render()[2]


@pytest.mark.parametrize("label", ["{{ task.remote_host | upper }}", None, ""])
def test_task_host_filter_preserves_inherited_label(balancer_multi, label):
    with pools(), variables({"password": "password"}):
        task = SSHTask.model_validate(
            {
                "task_id": "inherited-label",
                "command": "hostname",
                "template": SSHTaskArgs(map_index_template=label),
                "ssh_hook": BalancerHostQueryConfiguration(kind="filter", balancer=balancer_multi, tag="worker"),
            }
        )
    assert task.map_index_template == label


@pytest.mark.parametrize("version,expected", [("2.8.4", None), ("2.9.0", "{{ task.remote_host }}"), ("2.10.5", "{{ task.remote_host }}")])
def test_airflow_2_named_mapping_compatibility(balancer_multi, version, expected):
    with (
        patch("airflow_pydantic.operators.ssh._airflow_3", return_value=False),
        patch("airflow_pydantic.operators.ssh.version", return_value=version),
        pools(),
        variables({"password": "password"}),
    ):
        task = SSHTask(
            task_id="version-label",
            command="hostname",
            ssh_hook=BalancerHostQueryConfiguration(kind="filter", balancer=balancer_multi, tag="worker"),
        )
    assert task.map_index_template == expected
    assert ("map_index_template" in task.model_dump(exclude_unset=True)) == (expected is not None)
