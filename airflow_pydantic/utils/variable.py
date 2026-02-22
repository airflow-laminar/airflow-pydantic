from pydantic import (
    Field,
    model_validator,
)

from ..airflow import Variable as BaseVariable
from ..core import BaseModel

__all__ = ("Variable",)


class Variable(BaseModel):
    key: str = Field(description="Variable key")
    val: str | None = Field(default="", description="Variable value", alias="_val", exclude=True)
    description: str | None = Field(default="", description="Variable description")
    is_encrypted: bool | None = Field(default=False, description="Whether the variable is encrypted")

    # Not technically a field, but needed
    deserialize_json: bool | None = Field(default=False, description="Whether to deserialize JSON")

    @model_validator(mode="before")
    @classmethod
    def _validate_variable(cls, v):
        if isinstance(v, str):
            v = {"key": v}
        elif isinstance(v, BaseVariable):
            v = {"key": v.key, "val": v._val, "description": v.description, "is_encrypted": v.is_encrypted}
        return v

    def get(self):
        return BaseVariable.get(self.key, deserialize_json=self.deserialize_json)
