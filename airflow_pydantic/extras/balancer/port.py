from typing import List, Optional

from pydantic import Field, model_validator
from typing_extensions import Self

from ...core import BaseModel
from .host import Host

__all__ = ("Port",)


class Port(BaseModel):
    name: str = ""
    host: Optional[Host] = None
    host_name: str = ""
    port: int = Field(default=None, ge=1, le=65535, description="Port number")
    tags: List[str] = Field(default_factory=list)

    @property
    def _calc_name(self):
        if not self.name:
            if self.host:
                return f"{self.host.name}-{self.port}"
            return f"{self.host_name}-{self.port}"
        return self.name

    def __lt__(self, other):
        return self._calc_name < other._calc_name

    def __eq__(self, other):
        return self._calc_name == other._calc_name

    def __hash__(self):
        return hash(self._calc_name)

    @property
    def pool(self):
        return self._calc_name

    @model_validator(mode="after")
    def _validate(self) -> Self:
        if not self.host and not self.host_name:
            raise ValueError("Either host or host_name must be provided")
        if self.host and self.host_name and self.host.name != self.host_name:
            raise ValueError("Host and host_name must match")
        return self
