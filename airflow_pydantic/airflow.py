from getpass import getuser
from typing import Set

__all__ = (
    "BashOperator",
    "BashSensor",
    "BranchPythonOperator",
    "EmptyOperator",
    "get_parsing_context",
    "Param",
    "Pool",
    "PoolNotFound",
    "PythonOperator",
    "PythonSensor",
    "ShortCircuitOperator",
    "SSHHook",
    "SSHOperator",
    "TriggerRule",
    "_AirflowPydanticMarker",
)


class _AirflowPydanticMarker: ...


try:
    from airflow.models.param import Param  # noqa: F401
    from airflow.models.pool import Pool, PoolNotFound  # noqa: F401
    from airflow.models.variable import Variable  # noqa: F401
    from airflow.utils.dag_parsing_context import get_parsing_context  # noqa: F401
    from airflow.utils.trigger_rule import TriggerRule  # noqa: F401
except ImportError:
    from enum import Enum

    class TriggerRule(str, Enum):
        """Class with task's trigger rules."""

        ALL_SUCCESS = "all_success"
        ALL_FAILED = "all_failed"
        ALL_DONE = "all_done"
        ALL_DONE_SETUP_SUCCESS = "all_done_setup_success"
        ONE_SUCCESS = "one_success"
        ONE_FAILED = "one_failed"
        ONE_DONE = "one_done"
        NONE_FAILED = "none_failed"
        NONE_SKIPPED = "none_skipped"
        ALWAYS = "always"
        NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
        ALL_SKIPPED = "all_skipped"

        @classmethod
        def is_valid(cls, trigger_rule: str) -> bool:
            """Validate a trigger rule."""
            return trigger_rule in cls.all_triggers()

        @classmethod
        def all_triggers(cls) -> Set[str]:
            """Return all trigger rules."""
            return set(cls.__members__.values())

        def __str__(self) -> str:
            return self.value

    class Param(_AirflowPydanticMarker):
        def __init__(self, value=None, default=None, title=None, description=None, type=None, **kwargs):
            self.value = value or default
            self.default = value or default
            self.title = title
            self.description = description
            if not isinstance(type, list):
                type = [type]
            self.type = type
            self.schema = kwargs.pop(
                "schema",
                {
                    "value": value or default,
                    "title": title,
                    "description": description,
                    "type": type,
                },
            )

        def serialize(self) -> dict:
            return {"value": self.value, "description": self.description, "schema": self.schema}

    class Pool:
        def __init__(self, name: str, slots: int, description: str):
            self.name = name
            self.slots = slots
            self.description = description

        @classmethod
        def get_pool(cls, name: str) -> "Pool":
            # Simulate getting a pool from Airflow
            return cls(name=name, slots=5, description="Test pool")

        @classmethod
        def create_or_update_pool(cls, name: str, slots: int, description: str):
            # Simulate creating or updating a pool in Airflow
            pass

    class PoolNotFound(Exception):
        pass

    class Variable:
        @staticmethod
        def get(name: str, deserialize_json: bool = False):
            # Simulate getting a variable from Airflow
            if deserialize_json:
                return {"key": "value"}
            return "value"

    def get_parsing_context():
        # Airflow not installed, so no parsing context
        return None


# Operators
try:
    from airflow.operators.bash import BashOperator  # noqa: F401
    from airflow.operators.empty import EmptyOperator  # noqa: F401
    from airflow.operators.python import (
        BranchPythonOperator,  # noqa: F401
        PythonOperator,  # noqa: F401
        ShortCircuitOperator,  # noqa: F401
    )
    from airflow.sensors.bash import BashSensor  # noqa: F401
    from airflow.sensors.python import PythonSensor  # noqa: F401
except ImportError:

    class PythonOperator(_AirflowPydanticMarker): ...

    class BranchPythonOperator(_AirflowPydanticMarker): ...

    class ShortCircuitOperator(_AirflowPydanticMarker): ...

    class BashOperator(_AirflowPydanticMarker): ...

    class PythonSensor(_AirflowPydanticMarker): ...

    class BashSensor(_AirflowPydanticMarker): ...

    class EmptyOperator(_AirflowPydanticMarker): ...


# Providers
try:
    from airflow.providers.ssh.hooks.ssh import SSHHook  # noqa: F401
    from airflow.providers.ssh.operators.ssh import SSHOperator  # noqa: F401
except ImportError:

    class SSHHook(_AirflowPydanticMarker):
        def __init__(self, remote_host: str, username: str = None, password: str = None, key_file: str = None, **kwargs):
            self.remote_host = remote_host
            self.username = username or getuser()
            self.password = password
            self.key_file = key_file
            self.keepalive_interval = kwargs.pop("keepalive_interval", 30)

    class SSHOperator(_AirflowPydanticMarker): ...
