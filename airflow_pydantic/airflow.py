from typing import Any

try:
    from airflow.models.param import Param  # noqa: F401
    from airflow.utils.trigger_rule import TriggerRule  # noqa: F401
except ImportError:
    from enum import StrEnum

    class TriggerRule(StrEnum):
        ALL_SUCCESS = "all_success"
        ALL_FAILED = "all_failed"
        ANY_SUCCESS = "any_success"
        ANY_FAILED = "any_failed"
        NONE_FAILED = "none_failed"

    class Param:
        def __init__(self, default=None, title=None, description=None, type=None, **kwargs):
            self.value = default
            self.title = title
            self.description = description
            self.type = type
            self.schema = kwargs.pop(
                "schema",
                {
                    "value": default,
                    "default": default,
                    "title": title,
                    "description": description,
                    "type": type,
                },
            )

        def __copy__(self) -> Param:
            return Param(self.value, title=self.title, description=self.description, type=self.type)

        def dump(self) -> dict:
            """Dump the Param as a dictionary."""
            # TODO
            return {}

        @property
        def has_value(self) -> bool:
            return self.value is not None

        def serialize(self) -> dict:
            return {"value": self.value, "description": self.description, "schema": self.schema}

        @staticmethod
        def deserialize(data: dict[str, Any], version: int) -> Param:
            return Param(default=data["value"], description=data["description"], schema=data["schema"])
