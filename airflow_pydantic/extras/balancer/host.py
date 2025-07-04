from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import Field

from ...core import BaseModel
from ...utils import Pool, Variable

if TYPE_CHECKING:
    from airflow_pydantic.airflow import SSHHook


__all__ = ("Host",)


class Host(BaseModel):
    name: str
    username: Optional[str] = None

    # Password
    password: Optional[Union[str, Variable]] = None

    # Or get key file
    key_file: Optional[str] = None

    os: Optional[str] = None

    # Airflow / balance
    pool: Optional[Pool] = None
    size: Optional[int] = None
    queues: List[str] = Field(default_factory=list)

    tags: List[str] = Field(default_factory=list)

    def override(self, **kwargs) -> "Host":
        return Host(**{**self.model_dump(), **kwargs})

    def hook(self, username: str = None, use_local: bool = True, **hook_kwargs) -> "SSHHook":
        from airflow_pydantic.airflow import SSHHook

        if use_local and not self.name.count(".") > 0:
            name = f"{self.name}.local"
        else:
            name = self.name
        username = username or self.username
        if username and self.password:
            if isinstance(self.password, str):
                return SSHHook(remote_host=name, username=username, password=self.password, **hook_kwargs)
            password = self.password.get()
            if isinstance(password, dict):
                # TODO
                # Assume "password"
                password = password["password"]
            return SSHHook(remote_host=name, username=username, password=password, **hook_kwargs)
        elif username and self.key_file:
            return SSHHook(remote_host=name, username=username, key_file=self.key_file, **hook_kwargs)
        elif username:
            return SSHHook(remote_host=name, username=username, **hook_kwargs)
        else:
            return SSHHook(remote_host=name, **hook_kwargs)

    def __lt__(self, other):
        return self.name < other.name

    def __le__(self, other):
        return self.name <= other.name

    def __hash__(self):
        return hash(self.name)
