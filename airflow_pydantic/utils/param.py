from datetime import datetime, timedelta
from types import FunctionType, MethodType
from typing import Annotated, Any, Literal, Optional, get_origin

from pydantic import (
    BaseModel,
    Field,
    GetCoreSchemaHandler,
)
from pydantic_core.core_schema import (
    CoreSchema,
    any_schema,
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

from ..airflow import Param as BaseParam

__all__ = ("Param", "ParamType")


class ParamType:
    value: Optional[Any] = Field(
        default=None,
        description="Param value, can be any type",
    )
    title: Optional[str] = Field(
        default=None,
        description="Param title",
    )
    description: Optional[str] = Field(default=None, description="Param description")
    type: Optional[Literal["string", "number", "integer", "boolean", "array", "object", "null"]] = Field(
        default=None, description="Param type, e.g. 'string', 'integer', 'boolean', etc."
    )

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        types_schema = model_fields_schema(
            {
                "value": model_field(union_schema([any_schema(), none_schema()])),
                "title": model_field(union_schema([str_schema(), none_schema()])),
                "description": model_field(union_schema([str_schema(), none_schema()])),
                "type": model_field(union_schema([str_schema(), none_schema()])),
            },
            model_name="Param",
        )
        schema = union_schema([is_instance_schema(BaseParam), types_schema, no_info_plain_validator_function(cls._validate, ref=cls.__name__)])
        return json_or_python_schema(
            json_schema=schema,
            python_schema=schema,
            serialization=plain_serializer_function_ser_schema(cls._serialize, is_field_serializer=True, when_used="json"),
        )

    @classmethod
    def _validate(cls, v) -> BaseParam:
        return BaseParam(**v)

    @classmethod
    def _serialize(cls, info, value: BaseParam) -> dict:
        ret = {}
        for key in ParamType.__annotations__:
            val = getattr(value, key, getattr(value.schema, key, None))
            if val is not None:
                ret[key] = val
        return ret

    @classmethod
    def _resolve_type(cls, typ_) -> str:
        if isinstance(typ_, list):
            return "array"
        if isinstance(typ_, dict):
            return "object"
        if not isinstance(typ_, type):
            try:
                if get_origin(typ_) is dict:
                    # Handle generic dict types
                    return "object"
                if get_origin(typ_) is list:
                    # Handle generic list types
                    return "array"
            except Exception:
                # Ignore and return None
                ...
            return None
        if issubclass(typ_, bool):
            return "boolean"
        if issubclass(typ_, str):
            return "string"
        if issubclass(typ_, int):
            return "integer"
        if issubclass(typ_, float):
            return "number"
        if issubclass(typ_, list):
            return "array"
        if issubclass(typ_, datetime):
            # epoch
            return "number"
        if issubclass(typ_, timedelta):
            # seconds
            return "number"
        if typ_ is None:
            return "null"
        if issubclass(typ_, (FunctionType, MethodType)):
            return None
        if issubclass(typ_, BaseModel):
            return "object"
        # Can't resolve
        return None


Param = Annotated[BaseParam, ParamType]
