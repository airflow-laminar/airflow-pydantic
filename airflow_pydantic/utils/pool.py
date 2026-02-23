from pydantic import (
    Field,
    model_validator,
)

from ..airflow import Pool as BasePool
from ..core import BaseModel

__all__ = ("Pool",)


class Pool(BaseModel):
    pool: str = Field(
        description="Pool name",
    )
    slots: int | None = Field(
        default=None,
        description="Number of slots in the pool",
    )
    description: str | None = Field(default="", description="Pool description")
    include_deferred: bool | None = Field(default=False, description="Whether to include deferred tasks in the pool")

    @model_validator(mode="before")
    @classmethod
    def _validate_pool(cls, v):
        if isinstance(v, str):
            v = {"pool": v}
        elif isinstance(v, BasePool):
            v = {"pool": v.pool, "slots": v.slots, "description": v.description, "include_deferred": v.include_deferred}
        return v
