from typing import Dict, List, Optional

from pydantic import ConfigDict, Field, field_serializer, field_validator

from .task import _TaskSpecificArgs
from .utils import CallablePath

__all__ = (
    "PythonOperatorArgs",
    "BashOperatorArgs",
    "SSHOperatorArgs",
)


class PythonOperatorArgs(_TaskSpecificArgs):
    # python operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator
    python_callable: Optional[CallablePath] = Field(default=None, description="python_callable")
    op_args: Optional[List[object]] = Field(
        default=None, description="a list of positional arguments that will get unpacked when calling your callable"
    )
    op_kwargs: Optional[Dict[str, object]] = Field(
        default=None, description="a dictionary of keyword arguments that will get unpacked in your function"
    )
    templates_dict: Optional[Dict[str, object]] = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)",
    )
    templates_exts: Optional[List[str]] = Field(
        default=None, description="a list of file extensions to resolve while processing templated fields, for examples ['.sql', '.hql']"
    )
    show_return_value_in_logs: Optional[bool] = Field(
        default=None,
        description="a bool value whether to show return_value logs. Defaults to True, which allows return value log output. It can be set to False",
    )

    # generic extras
    model_config = ConfigDict(extra="allow")


class BashOperatorArgs(_TaskSpecificArgs):
    # bash operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html
    bash_command: Optional[str] = Field(default=None, description="bash_command")
    env: Optional[Dict[str, str]] = Field(default=None)
    append_env: Optional[bool] = Field(default=False)
    output_encoding: Optional[str] = Field(default="utf-8")
    skip_exit_code: Optional[bool] = Field(default=None)
    skip_on_exit_code: Optional[int] = Field(default=99)
    cwd: Optional[str] = Field(default=None)
    output_processor: Optional[CallablePath] = None

    # generic extras
    model_config = ConfigDict(extra="allow")


class SSHOperatorArgs(_TaskSpecificArgs):
    # ssh operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html
    ssh_hook: Optional[CallablePath] = Field(
        default=None, description="predefined ssh_hook to use for remote execution. Either ssh_hook or ssh_conn_id needs to be provided."
    )
    ssh_conn_id: Optional[str] = Field(
        default=None, description="ssh connection id from airflow Connections. ssh_conn_id will be ignored if ssh_hook is provided."
    )
    remote_host: Optional[str] = Field(
        default=None,
        description="remote host to connect (templated) Nullable. If provided, it will replace the remote_host which was defined in ssh_hook or predefined in the connection of ssh_conn_id.",
    )
    command: Optional[str] = Field(default=None, description="command to execute on remote host. (templated)")
    conn_timeout: Optional[int] = Field(
        default=None,
        description="timeout (in seconds) for maintaining the connection. The default is 10 seconds. Nullable. If provided, it will replace the conn_timeout which was predefined in the connection of ssh_conn_id.",
    )
    cmd_timeout: Optional[int] = Field(
        default=None,
        description="timeout (in seconds) for executing the command. The default is 10 seconds. Nullable, None means no timeout. If provided, it will replace the cmd_timeout which was predefined in the connection of ssh_conn_id.",
    )
    environment: Optional[Dict[str, str]] = Field(
        default=None,
        description="a dict of shell environment variables. Note that the server will reject them silently if AcceptEnv is not set in SSH config. (templated)",
    )
    get_pty: Optional[bool] = Field(
        default=None,
        description="request a pseudo-terminal from the server. Set to True to have the remote process killed upon task timeout. The default is False but note that get_pty is forced to True when the command starts with sudo.",
    )
    banner_timeout: Optional[int] = Field(default=None, description="timeout to wait for banner from the server in seconds")
    skip_on_exit_code: Optional[int] = Field(
        default=None,
        description="If command exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.",
    )

    @field_validator("ssh_hook")
    @classmethod
    def _validate_ssh_hook(cls, v):
        if v:
            from airflow.providers.ssh.hooks.ssh import SSHHook

            assert isinstance(v, SSHHook)
        return v

    @field_serializer("ssh_hook")
    def _serialize_hook(self, ssh_hook: object):
        if ssh_hook is not None:
            return f"SSHHook(hostname={ssh_hook.hostname}, ssh_conn_id={ssh_hook.ssh_conn_id})"
        return None

    # generic extras
    model_config = ConfigDict(extra="allow")
