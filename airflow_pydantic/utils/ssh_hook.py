from typing import Annotated, Any, Optional

from pydantic import (
    Field,
    GetCoreSchemaHandler,
)
from pydantic_core.core_schema import (
    CoreSchema,
    float_schema,
    int_schema,
    is_instance_schema,
    json_or_python_schema,
    model_field,
    model_fields_schema,
    no_info_plain_validator_function,
    none_schema,
    plain_serializer_function_ser_schema,
    str_schema,
    union_schema,
)

from ..airflow import SSHHook as BaseSSHHook

__all__ = ("SSHHook",)


class SSHHookType:
    ssh_conn_id: Optional[str] = Field(
        description="Connections from where all the required parameters can be fetched like username, password or key_file, though priority is given to the params passed during init."
    )
    remote_host: Optional[str] = Field(default="", description="remote host to connect")
    username: Optional[str] = Field(description="username to connect to the remote_host")
    password: Optional[str] = Field(description="password of the username to connect to the remote_host")
    key_file: Optional[str] = Field(description="path to key file to use to connect to the remote_host")
    port: Optional[int] = Field(default=22, description="port of remote host to connect (Default is paramiko SSH_PORT)")
    conn_timeout: Optional[int] = Field(description="timeout (in seconds) for the attempt to connect to the remote_host.")
    cmd_timeout: Optional[float] = Field(default=10, description="timeout (in seconds) for executing the command. The default is 10 seconds.")
    keepalive_interval: Optional[int] = Field(default=30, description="send a keepalive packet to remote host every keepalive_interval seconds")
    banner_timeout: Optional[float] = Field(default=30, description="timeout to wait for banner from the server in seconds")
    # disabled_algorithms: Optional[dict[str, Any]] = Field(description="dictionary mapping algorithm type to an iterable of algorithm identifiers, which will be disabled for the lifetime of the transport")
    # ciphers: Optional[list[str]] = Field(description="list of ciphers to use in order of preference")
    auth_timeout: Optional[int] = Field(description="timeout (in seconds) for the attempt to authenticate with the remote_host")
    # host_proxy_cmd: Optional[str] = Field(description="")

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        types_schema = model_fields_schema(
            {
                "ssh_conn_id": model_field(union_schema([str_schema(), none_schema()])),
                "remote_host": model_field(union_schema([str_schema(), none_schema()])),
                "username": model_field(union_schema([str_schema(), none_schema()])),
                "password": model_field(union_schema([str_schema(), none_schema()])),
                "key_file": model_field(union_schema([str_schema(), none_schema()])),
                "port": model_field(union_schema([int_schema(), none_schema()])),
                "conn_timeout": model_field(union_schema([int_schema(), none_schema()])),
                "cmd_timeout": model_field(union_schema([float_schema(), none_schema()])),
                "keepalive_interval": model_field(union_schema([int_schema(), none_schema()])),
                "banner_timeout": model_field(union_schema([float_schema(), none_schema()])),
                "auth_timeout": model_field(union_schema([int_schema(), none_schema()])),
            },
            model_name="SSHHook",
        )
        schema = union_schema([is_instance_schema(BaseSSHHook), types_schema, no_info_plain_validator_function(cls._validate, ref=cls.__name__)])
        return json_or_python_schema(
            json_schema=schema,
            python_schema=schema,
            serialization=plain_serializer_function_ser_schema(cls._serialize, is_field_serializer=True, when_used="json"),
        )

    @classmethod
    def _validate(cls, v) -> BaseSSHHook:
        return BaseSSHHook(**v)

    @classmethod
    def _serialize(cls, info, value: BaseSSHHook) -> dict:
        ret = {}
        for key in SSHHookType.__annotations__:
            val = getattr(value, key, None)
            if val is not None:
                ret[key] = val
        return ret


SSHHook = Annotated[BaseSSHHook, SSHHookType]
