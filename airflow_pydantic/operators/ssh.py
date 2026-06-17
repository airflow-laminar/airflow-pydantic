import ast
from logging import getLogger
from types import FunctionType, MethodType
from typing import Any

from pydantic import Field, TypeAdapter, field_validator, model_validator

from ..airflow import SSHHook as BaseSSHHook
from ..core import Task, TaskArgs
from ..extras import BalancerHostQueryConfiguration, Host
from ..utils import BashCommands, CallablePath, ImportPath, SSHHook, Variable, get_import_path

__all__ = (
    "SSHOperator",
    "SSHOperatorArgs",
    "SSHTask",
    "SSHTaskArgs",
)

_log = getLogger(__name__)


class SSHTaskArgs(TaskArgs):
    # ssh operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html
    ssh_hook: SSHHook | CallablePath | BalancerHostQueryConfiguration | Host | None = Field(
        default=None, description="predefined ssh_hook to use for remote execution. Either ssh_hook or ssh_conn_id needs to be provided."
    )
    ssh_hook_host: Host | None = Field(default=None, exclude=True)

    # Hosts to fan out over when ssh_hook is a host filter (BalancerHostQueryConfiguration kind="filter").
    # Rendered / instantiated as `Operator.partial(...).expand(remote_host=[...])`.
    ssh_hook_hosts: list[Host] | None = Field(default=None, exclude=True)

    # Track source of hook in order to defer
    ssh_hook_foo: CallablePath | None = Field(default=None, exclude=True)
    ssh_hook_external: bool | None = Field(
        default=False, exclude=True, description="Whether to force the ssh_hook to be an external call or not. Only works when ssh_hook is a Callable"
    )

    ssh_conn_id: str | None = Field(
        default=None, description="ssh connection id from airflow Connections. ssh_conn_id will be ignored if ssh_hook is provided."
    )
    remote_host: str | None = Field(
        default=None,
        description="remote host to connect (templated) Nullable. If provided, it will replace the remote_host which was defined in ssh_hook or predefined in the connection of ssh_conn_id.",
    )
    command: str | list[str] | BashCommands = Field(default=None, description="command to execute on remote host. (templated)")
    conn_timeout: int | None = Field(
        default=None,
        description="timeout (in seconds) for maintaining the connection. The default is 10 seconds. Nullable. If provided, it will replace the conn_timeout which was predefined in the connection of ssh_conn_id.",
    )
    cmd_timeout: int | None = Field(
        default=None,
        description="timeout (in seconds) for executing the command. The default is 10 seconds. Nullable, None means no timeout. If provided, it will replace the cmd_timeout which was predefined in the connection of ssh_conn_id.",
    )
    environment: dict[str, str] | None = Field(
        default=None,
        description="a dict of shell environment variables. Note that the server will reject them silently if AcceptEnv is not set in SSH config. (templated)",
    )
    get_pty: bool | None = Field(
        default=None,
        description="request a pseudo-terminal from the server. Set to True to have the remote process killed upon task timeout. The default is False but note that get_pty is forced to True when the command starts with sudo.",
    )
    banner_timeout: int | None = Field(default=None, description="timeout to wait for banner from the server in seconds")
    skip_on_exit_code: int | None = Field(
        default=None,
        description="If command exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.",
    )

    @field_validator("command")
    @classmethod
    def validate_command(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v
        elif isinstance(v, list) and all(isinstance(item, str) for item in v):
            return BashCommands(commands=v)
        elif isinstance(v, BashCommands):
            return v
        else:
            raise ValueError("command must be a string, list of strings, or a BashCommands model")

    @staticmethod
    def _assert_uniform_credentials(hosts: list[Host]) -> None:
        # All hosts in a fan-out are reached through a single base SSHHook (only `remote_host` is
        # mapped per task instance), so they must agree on username / password / key_file.
        def _creds(host: Host):
            password = host.password
            password_key = password.key if isinstance(password, Variable) else password
            return (host.username, password_key, host.key_file)

        if len({_creds(host) for host in hosts}) > 1:
            names = ", ".join(sorted(host.name for host in hosts))
            raise ValueError(
                f"Host filter for SSH expansion matched hosts with differing credentials ({names}); "
                "all matching hosts must share username/password/key_file."
            )

    @model_validator(mode="before")
    @classmethod
    def _extract_host_from_ssh_hook(cls, data: Any) -> Any:
        if isinstance(data, dict) and "ssh_hook" in data:
            ssh_hook = data["ssh_hook"]
            if isinstance(ssh_hook, (BalancerHostQueryConfiguration, Host)):
                if isinstance(ssh_hook, BalancerHostQueryConfiguration):
                    if ssh_hook.kind == "select":
                        # Execute the query to get the Host, just set as it will
                        # be handled by the field validator
                        data["ssh_hook"] = ssh_hook.execute()

                        # Save the host for later use
                        data["ssh_hook_host"] = data["ssh_hook"]
                    elif ssh_hook.kind == "filter":
                        # Fan out: run this task on EVERY matching host via Airflow dynamic task
                        # mapping (rendered as `.partial(...).expand(remote_host=[...])`).
                        hosts = ssh_hook.execute()
                        if isinstance(hosts, Host):
                            hosts = [hosts]
                        # A single base hook supplies the shared credentials while only
                        # `remote_host` is mapped, so all matching hosts must share credentials.
                        cls._assert_uniform_credentials(hosts)
                        if data.get("remote_host"):
                            raise ValueError("Cannot set 'remote_host' together with a host filter; remote_host is mapped per-host during expansion.")
                        data["ssh_hook_hosts"] = hosts
                        # Base hook carries the shared credentials; remote_host is overridden per mapped instance.
                        data["ssh_hook"] = hosts[0]
                        data["ssh_hook_host"] = hosts[0]
                    else:
                        raise ValueError("BalancerHostQueryConfiguration must be of kind 'select' or 'filter'")
                else:
                    # If it's a Host instance, set it for later use
                    data["ssh_hook_host"] = ssh_hook

                # Override pool from host if not otherwise set.
                # Skipped when fanning out: per-host pools cannot be mapped over a single task.
                if not data.get("ssh_hook_hosts") and data["ssh_hook_host"].pool and not data.get("pool"):
                    data["pool"] = data["ssh_hook"].pool

            if isinstance(ssh_hook, str):
                # If ssh_hook is a string, we assume it's an import path
                data["ssh_hook_foo"] = get_import_path(ssh_hook)

                try:
                    data["ssh_hook"] = data["ssh_hook_foo"]()
                except Exception:  # noqa: BLE001
                    # Skip, might only run in situ
                    data["ssh_hook"] = None

            if isinstance(ssh_hook, (FunctionType, MethodType)):
                # If ssh_hook is a callable, we need to call it to get the SSHHook instance
                data["ssh_hook_foo"] = get_import_path(ssh_hook)

                try:
                    data["ssh_hook"] = data["ssh_hook_foo"]()
                except Exception:  # noqa: BLE001
                    # Skip, might only run in situ
                    data["ssh_hook"] = None
        return data

    @field_validator("ssh_hook", mode="before")
    @classmethod
    def _validate_ssh_hook(cls, v):
        if v:
            if isinstance(v, str):
                v = get_import_path(v)

            if isinstance(v, (FunctionType, MethodType)):
                try:
                    # If it's a callable, we need to call it to get the SSHHook instance
                    v = v()
                except Exception:  # noqa: BLE001
                    # Skip, might only run in situ
                    _log.info("Failed to call ssh_hook callable: %s", v)
                    v = None

            try:
                if isinstance(v, BalancerHostQueryConfiguration):
                    if not v.kind == "select":
                        raise ValueError("BalancerHostQueryConfiguration must be of kind 'select'")
                    v = v.execute().hook()
                if isinstance(v, (Host,)):
                    v = v.hook()
            except Exception:  # noqa: BLE001
                # Skip, might only run in situ
                _log.info("Failed to execute BalancerHostQueryConfiguration or Host: %s", v)
                v = None

            if isinstance(v, dict):
                v = TypeAdapter(SSHHook).validate_python(v)
            assert v is None or isinstance(v, BaseSSHHook), f"ssh_hook must be an instance of SSHHook, got: {type(v)}"
        return v

    # @field_serializer("ssh_hook", when_used="json")
    # def _serialize_ssh_hook(self, v: Optional[BaseSSHHook]) -> Optional[Dict[str, Any]]:
    #     import pdb; pdb.set_trace()
    #     if v is None:
    #         return None
    #     return SSHHook.__get_pydantic_core_schema__().serialization.serialize_json(SSHHook, v)


# Alias
SSHOperatorArgs = SSHTaskArgs


class SSHTask(Task, SSHTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.SSHOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import SSHOperator, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.ssh.operators.ssh.SSHOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("SSHOperator is a marker class, returning as is")
            return v
        if not issubclass(v, SSHOperator):
            raise TypeError(f"operator must be 'airflow.providers.ssh.operators.ssh.SSHOperator', got: {v}")
        return v

    @staticmethod
    def _remote_host_name(host: Host) -> str:
        # Mirror Host.hook(use_local=True): append ".local" to an unqualified hostname.
        return f"{host.name}.local" if host.name.count(".") == 0 else host.name

    def instantiate(self, **kwargs):
        # When ssh_hook resolves to a host filter, fan out across every matching host using Airflow
        # dynamic task mapping. A single base hook supplies the shared credentials and only
        # `remote_host` is mapped, producing one (independently retryable) task instance per host.
        if not self.ssh_hook_hosts:
            return super().instantiate(**kwargs)

        from ..airflow import Pool as AirflowPool
        from ..utils import Pool

        args = {**self.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]), **kwargs}
        if "pool" in args:
            if isinstance(args["pool"], dict):
                args["pool"] = self.pool
            if isinstance(args["pool"], (AirflowPool, Pool)):
                args["pool"] = args["pool"].pool
        # `remote_host` is supplied per mapped task instance, not in the partial.
        args.pop("remote_host", None)
        remote_hosts = [self._remote_host_name(host) for host in self.ssh_hook_hosts]
        return self.operator.partial(**args).expand(remote_host=remote_hosts)

    def render(self, raw: bool = False, dag_from_context: bool = False, airflow_major_version: int = 2, **kwargs):
        # Default (single-host) rendering is unchanged.
        if not self.ssh_hook_hosts:
            return super().render(raw=raw, dag_from_context=dag_from_context, airflow_major_version=airflow_major_version, **kwargs)

        # Build the regular `Operator(**args)` call, then rewrite it as
        # `Operator.partial(**args).expand(remote_host=[...])`.
        imports, globals_, call = super().render(raw=True, dag_from_context=dag_from_context, airflow_major_version=airflow_major_version, **kwargs)

        call.keywords = [keyword for keyword in call.keywords if keyword.arg != "remote_host"]
        partial_call = ast.Call(
            func=ast.Attribute(value=call.func, attr="partial", ctx=ast.Load()),
            args=call.args,
            keywords=call.keywords,
        )
        remote_hosts = [self._remote_host_name(host) for host in self.ssh_hook_hosts]
        call = ast.Call(
            func=ast.Attribute(value=partial_call, attr="expand", ctx=ast.Load()),
            args=[],
            keywords=[
                ast.keyword(
                    arg="remote_host",
                    value=ast.List(elts=[ast.Constant(value=name) for name in remote_hosts], ctx=ast.Load()),
                )
            ],
        )

        if not raw:
            imports = [ast.unparse(i) for i in imports]
            globals_ = [ast.unparse(i) for i in globals_]
            call = ast.unparse(call)
        return imports, globals_, call


# Alias
SSHOperator = SSHTask
