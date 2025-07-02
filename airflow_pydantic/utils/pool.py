from typing import Annotated, Any

from pydantic import (
    Field,
    GetCoreSchemaHandler,
)
from pydantic_core import core_schema

from ..airflow import Pool as BasePool

__all__ = ("Pool", "PoolType")


class PoolType:
    pool: str = Field(
        description="Pool name",
    )
    slots: int = Field(
        default=None,
        description="Number of slots in the pool",
    )
    description: str = Field(default="", description="Pool description")
    include_deferred: bool = Field(default=False, description="Whether to include deferred tasks in the pool")

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        types_schema = core_schema.model_fields_schema(
            {
                "pool": core_schema.model_field(core_schema.str_schema()),
                "slots": core_schema.model_field(core_schema.int_schema()),
                "description": core_schema.model_field(core_schema.str_schema()),
                "include_deferred": core_schema.model_field(core_schema.bool_schema()),
            },
            model_name="Pool",
        )
        union_schema = core_schema.union_schema(
            [core_schema.is_instance_schema(BasePool), types_schema, core_schema.no_info_plain_validator_function(cls._validate, ref=cls.__name__)]
        )
        return core_schema.json_or_python_schema(
            json_schema=union_schema,
            python_schema=union_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(cls._serialize, is_field_serializer=True, when_used="json"),
        )

    @classmethod
    def _validate(cls, v) -> BasePool:
        return BasePool(**v)

    @classmethod
    def _serialize(cls, info, value: BasePool) -> dict:
        ret = {}
        for key in PoolType.__annotations__:
            val = getattr(value, key, None)
            if val is not None:
                ret[key] = val
        return ret


Pool = Annotated[BasePool, PoolType]
