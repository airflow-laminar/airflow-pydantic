from getpass import getuser
from unittest.mock import patch

import pytest

from airflow_pydantic import BalancerConfiguration, Host, Variable
from airflow_pydantic.airflow import PoolNotFound
from airflow_pydantic.extras.balancer.host import UNRESOLVED_PASSWORD
from airflow_pydantic.testing import pools, variables


class TestConfig:
    @pytest.mark.parametrize("credentials", [{}, {"password": "secret"}, {"key_file": "/tmp/test-key"}, {"password": Variable(key="password")}])
    def test_saved_hook_options_and_call_time_precedence(self, credentials):
        host = Host(name="test", username="test", ssh_hook_args={"port": 2222, "keepalive_interval": 15, "conn_timeout": 9}, **credentials)
        with variables("secret"):
            hook = host.hook()
            override = host.hook(keepalive_interval=5)
        assert hook.port == 2222
        assert hook.keepalive_interval == 15
        assert hook.conn_timeout == 9
        assert override.keepalive_interval == 5
        assert host.ssh_hook_args["keepalive_interval"] == 15
        assert Host.model_validate_json(host.model_dump_json()).ssh_hook_args == host.ssh_hook_args
        assert host.override(username="other").ssh_hook_args == host.ssh_hook_args

    def test_hook_options_precedence_for_connection_fields(self):
        host = Host(name="host.example", username="user", ssh_hook_args={"remote_host": "ignored.example", "username": "fallback"})
        assert host.hook().remote_host == "host.example"
        assert host.hook().username == "user"
        assert host.hook(username="override", remote_host="override.example").remote_host == "override.example"
        assert host.hook(username="override").username == "override"

    def test_saved_key_file_is_preserved(self):
        host = Host(name="test", ssh_hook_args={"key_file": "/tmp/saved-key"})
        assert host.hook().key_file == "/tmp/saved-key"

    def test_saved_provider_options_are_forwarded(self):
        options = {"disabled_algorithms": {"pubkeys": ["ssh-rsa"]}}
        host = Host(name="test", ssh_hook_args=options)
        with patch("airflow_pydantic.airflow.SSHHook") as hook:
            host.hook()
        hook.assert_called_once_with(remote_host="test.local", **options)

    def test_yaml_host_options_reach_rendered_and_instantiated_ssh_task(self):
        from hydra.utils import instantiate
        from omegaconf import OmegaConf

        from airflow_pydantic import Dag, SSHTask
        from airflow_pydantic.migration import _airflow_3

        options = {"port": 2222, "keepalive_interval": 15, "cmd_timeout": None, "disabled_algorithms": {"pubkeys": ["ssh-rsa"]}}
        host = instantiate(OmegaConf.create({"_target_": "airflow_pydantic.Host", "name": "host.example", "ssh_hook_args": options}))
        task = SSHTask(task_id="run", command="echo ok", ssh_hook=host)
        dag = Dag(dag_id="saved-hook-options", schedule=None, tasks={"run": task})
        rendered = dag.render()
        assert "port=2222" in rendered
        assert "keepalive_interval=15" in rendered
        assert "cmd_timeout=None" in rendered
        assert "disabled_algorithms=" in rendered
        if _airflow_3() is not None:
            hook = dag.instantiate().get_task("run").ssh_hook
            assert hook.port == 2222
            assert hook.keepalive_interval == 15
            namespace = {}
            exec(rendered, namespace)
            rendered_hook = namespace["dag"].get_task("run").ssh_hook
            assert rendered_hook.cmd_timeout is None
            assert rendered_hook.disabled_algorithms == options["disabled_algorithms"]

    def test_saved_options_preserve_runtime_password_lookup(self):
        from airflow_pydantic import SSHTask

        host = Host(name="host.example", username="test", password=Variable(key="secret"), ssh_hook_args={"port": 2222})
        with variables(side_effect=RuntimeError("offline")):
            task = SSHTask(task_id="run", command="echo ok", ssh_hook=host)
        _, _, rendered = task.render()
        assert "port=2222" in rendered
        assert "AirflowVariable.get('secret')" in rendered
        assert UNRESOLVED_PASSWORD not in rendered

    def test_host_filter_rejects_different_saved_options(self):
        from airflow_pydantic import BalancerHostQueryConfiguration, SSHTask

        balancer = BalancerConfiguration(hosts=[Host(name="one", ssh_hook_args={"port": 22}), Host(name="two", ssh_hook_args={"port": 2222})])
        with pytest.raises(ValueError, match="SSHHook arguments"):
            SSHTask(command="echo ok", ssh_hook=BalancerHostQueryConfiguration(kind="filter", balancer=balancer))

    def test_host_override(self):
        h = Host(name="test", username="test")

        assert h.hook()
        assert h.hook().username == "test"

        h2 = h.override(username="test2")
        assert h2.hook()
        assert h2.hook().username == "test2"

    def test_hook_kwargs(self):
        h = Host(name="test", username="test")
        assert h.hook().keepalive_interval == 30
        assert h.hook(keepalive_interval=10).keepalive_interval == 10

    def test_load_hook(self):
        h = Host(name="test", username="test")

        assert h.hook()
        assert h.hook().username == "test"
        assert h.hook().remote_host == "test.local"
        assert h.hook().password is None
        assert h.hook().key_file is None

        h = Host(name="test", username="test", password="test")
        assert h.hook()
        assert h.hook().username == "test"
        assert h.hook().remote_host == "test.local"
        assert h.hook().password == "test"
        assert h.hook().key_file is None

        h = Host(name="test", username="test", password=Variable(key="test"))
        with variables("test"):
            assert h.hook()
            assert h.hook().username == "test"
            assert h.hook().remote_host == "test.local"
            assert h.hook().password == "test"
            assert h.hook().key_file is None

        h = Host(name="test", username="test", password=Variable(key="test", deserialize_json=True))
        with variables({"password": "blerg"}, side_effect=lambda *args, **kwargs: {"password": "blerg"}):
            assert h.hook()
            assert h.hook().username == "test"
            assert h.hook().remote_host == "test.local"
            assert h.hook().password == "blerg"
            assert h.hook().key_file is None

        h = Host(name="test", username="test", key_file="test")
        assert h.hook()
        assert h.hook().username == "test"
        assert h.hook().remote_host == "test.local"
        assert h.hook().password is None
        assert h.hook().key_file == "test"

        h = Host(name="test")
        assert h.hook()
        assert h.hook().username == getuser()
        assert h.hook().remote_host == "test.local"
        assert h.hook().password is None
        assert h.hook().key_file is None

    def test_load_hook_unresolvable_variable(self):
        h = Host(name="test", username="test", password=Variable(key="test"))

        with variables(side_effect=RuntimeError("no metadata database")):
            hook = h.hook()

        assert hook.username == "test"
        assert hook.remote_host == "test.local"
        assert hook.password == UNRESOLVED_PASSWORD

    def test_filter_hosts(self):
        with pools(side_effect=PoolNotFound()):
            b = BalancerConfiguration(
                hosts=[
                    Host(name="host1", os="ab", queues=["ab"], tags=["tag1", "tag2"]),
                    Host(name="host2", os="abc", queues=["abc"], tags=["tg2"]),
                    Host(name="host3", os="ac", queues=["ac"], tags=["tag3"]),
                ]
            )

            assert len(b.filter_hosts(queue="ab")) == 1
            assert len(b.filter_hosts(queue=["abc"])) == 1
            assert len(b.filter_hosts(queue=["ac"])) == 1
            assert len(b.filter_hosts(queue=["ab", "abc"])) == 2
            assert len(b.filter_hosts(queue="ab*")) == 2

            assert b.select_host(queue="ab") in b.filter_hosts(queue="ab")
            assert b.select_host(queue=["abc"]) in b.filter_hosts(queue=["abc"])
            assert b.select_host(queue=["ac"]) in b.filter_hosts(queue=["ac"])
            assert b.select_host(queue=["ab", "abc"]) in b.filter_hosts(queue=["ab", "abc"])
            assert b.select_host(queue="ab*") in b.filter_hosts(queue="ab*")

            assert len(b.filter_hosts(tag="tag1")) == 1
            assert len(b.filter_hosts(tag="tag*")) == 2
            assert len(b.filter_hosts(tag="t*")) == 3
            assert len(b.filter_hosts(tag=["tag1", "tag2"])) == 1

            assert b.select_host(tag="tag1") in b.filter_hosts(tag="tag1")
            assert b.select_host(tag="tag*") in b.filter_hosts(tag="tag*")
            assert b.select_host(tag="t*") in b.filter_hosts(tag="t*")
            assert b.select_host(tag=["tag1", "tag2"]) in b.filter_hosts(tag=["tag1", "tag2"])

            assert len(b.filter_hosts(os="ab")) == 1
            assert len(b.filter_hosts(os="ab*")) == 2
            assert len(b.filter_hosts(os="abc")) == 1
            assert len(b.filter_hosts(os=["ab*", "ac"])) == 3

            assert b.select_host(os="ab") in b.filter_hosts(os="ab")
            assert b.select_host(os="ab*") in b.filter_hosts(os="ab*")
            assert b.select_host(os="abc") in b.filter_hosts(os="abc")
            assert b.select_host(os=["ab*", "ac"]) in b.filter_hosts(os=["ab*", "ac"])
