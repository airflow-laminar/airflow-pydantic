from typing import Annotated, Any, Optional

from dateutil.relativedelta import relativedelta, weekday as weekdaytype
from pydantic import Field, GetCoreSchemaHandler, model_serializer, model_validator
from pydantic_core.core_schema import (
    CoreSchema,
    ValidatorFunctionWrapHandler,
    int_schema,
    is_instance_schema,
    json_or_python_schema,
    model_field,
    model_fields_schema,
    no_info_plain_validator_function,
    none_schema,
    plain_serializer_function_ser_schema,
    union_schema,
)

__all__ = (
    "Weekday",
    "RelativeDelta",
)


class WeekdayType:
    weekday: int = Field(ge=0, le=6)
    n: Optional[int]

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        types_schema = model_fields_schema(
            {
                "weekday": model_field(union_schema([int_schema(), none_schema()])),
                "n": model_field(union_schema([int_schema(), none_schema()])),
            },
            model_name="Weekday",
        )
        schema = union_schema([is_instance_schema(weekdaytype), types_schema, no_info_plain_validator_function(cls._validate, ref=cls.__name__)])
        return json_or_python_schema(
            json_schema=schema,
            python_schema=schema,
            serialization=plain_serializer_function_ser_schema(cls._serialize, is_field_serializer=True, when_used="json"),
        )

    @classmethod
    def _validate(cls, v) -> weekdaytype:
        return weekdaytype(**v)

    @classmethod
    def _serialize(cls, info, value: weekdaytype) -> dict:
        ret = {}
        for key in WeekdayType.__annotations__:
            val = getattr(value, key, None)
            if val is not None:
                ret[key] = val
        return ret

    @model_validator(mode="wrap")
    def _validate(value, handler: ValidatorFunctionWrapHandler) -> relativedelta:
        # if already dateutil._common.weekday instance, return it
        if isinstance(value, weekdaytype):
            return value

        # otherwise run model validation, which returns either a
        # a dateutil._common.weekday or a WeekdayAnnotations
        validated = handler(value)
        if isinstance(validated, weekdaytype):
            return validated

        kwargs = {k: v for k, v in dict(validated).items() if v is not None}
        return weekdaytype(**kwargs)

    @model_serializer(mode="plain")
    def _serialize(self: weekdaytype) -> dict[str, Any]:
        return {"weekday": self.weekday, "n": self.n}


Weekday = Annotated[weekdaytype, WeekdayType]


class RelativeDeltaType:
    years: Optional[int] = None
    months: Optional[int] = None
    days: Optional[int] = None
    hours: Optional[int] = None
    minutes: Optional[int] = None
    seconds: Optional[int] = None
    microseconds: Optional[int] = None

    year: Optional[int] = None
    month: Optional[int] = Field(ge=1, le=12)
    day: Optional[int] = Field(ge=1, le=31)
    hour: Optional[int] = Field(ge=0, le=23)
    minute: Optional[int] = Field(ge=0, le=59)
    second: Optional[int] = Field(ge=0, le=59)
    microsecond: Optional[int] = Field(ge=0, le=999999)
    weekday: Optional[Weekday] = None
    leapdays: Optional[int] = None

    # validation only fields
    yearday: Optional[int] = Field(None, exclude=True)
    nlyearday: Optional[int] = Field(None, exclude=True)
    weeks: Optional[int] = Field(None, exclude=True)
    dt1: Optional[int] = Field(None, exclude=True)
    dt2: Optional[int] = Field(None, exclude=True)

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        types_schema = model_fields_schema(
            {
                "years": model_field(union_schema([int_schema(), none_schema()])),
                "months": model_field(union_schema([int_schema(), none_schema()])),
                "days": model_field(union_schema([int_schema(), none_schema()])),
                "hours": model_field(union_schema([int_schema(), none_schema()])),
                "minutes": model_field(union_schema([int_schema(), none_schema()])),
                "seconds": model_field(union_schema([int_schema(), none_schema()])),
                "microseconds": model_field(union_schema([int_schema(), none_schema()])),
                "year": model_field(union_schema([int_schema(), none_schema()])),
                "month": model_field(union_schema([int_schema(), none_schema()])),
                "day": model_field(union_schema([int_schema(), none_schema()])),
                "hour": model_field(union_schema([int_schema(), none_schema()])),
                "minute": model_field(union_schema([int_schema(), none_schema()])),
                "second": model_field(union_schema([int_schema(), none_schema()])),
                "microsecond": model_field(union_schema([int_schema(), none_schema()])),
                "weekday": model_field(union_schema([int_schema(), none_schema()])),
                "leapdays": model_field(union_schema([int_schema(), none_schema()])),
                "yearday": model_field(union_schema([int_schema(), none_schema()])),
                "nlyearday": model_field(union_schema([int_schema(), none_schema()])),
                "weeks": model_field(union_schema([int_schema(), none_schema()])),
                "dt1": model_field(union_schema([int_schema(), none_schema()])),
                "dt2": model_field(union_schema([int_schema(), none_schema()])),
            },
            model_name="SSHHook",
        )
        schema = union_schema([is_instance_schema(relativedelta), types_schema, no_info_plain_validator_function(cls._validate, ref=cls.__name__)])
        return json_or_python_schema(
            json_schema=schema,
            python_schema=schema,
            serialization=plain_serializer_function_ser_schema(cls._serialize, is_field_serializer=True, when_used="json"),
        )

    @classmethod
    def _validate(cls, v) -> relativedelta:
        return relativedelta(**v)

    @classmethod
    def _serialize(cls, info, value: relativedelta) -> dict:
        ret = {}
        for key in RelativeDeltaType.__annotations__:
            val = getattr(value, key, None)
            if val is not None:
                ret[key] = val
        return ret


RelativeDelta = Annotated[relativedelta, RelativeDeltaType]
