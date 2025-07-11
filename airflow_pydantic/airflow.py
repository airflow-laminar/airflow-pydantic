from datetime import timedelta
from enum import Enum
from getpass import getuser
from logging import getLogger
from typing import Set

from .migration import _airflow_3

__all__ = (
    "AirflowFailException",
    "AirflowSkipException",
    "BashOperator",
    "BashSensor",
    "BranchPythonOperator",
    "CronDataIntervalTimetable",
    "CronTriggerTimetable",
    "DeltaDataIntervalTimetable",
    "EmptyOperator",
    "EventsTimetable",
    "MultipleCronTriggerTimetable",
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
    "Variable",
    "_AirflowPydanticMarker",
)

_log = getLogger(__name__)


class _AirflowPydanticMarker: ...


if _airflow_3():
    _log.info("Using Airflow 3.x imports")
    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.models.param import Param  # noqa: F401
    from airflow.models.pool import Pool, PoolNotFound  # noqa: F401
    from airflow.models.variable import Variable  # noqa: F401
    from airflow.providers.ssh.hooks.ssh import SSHHook  # noqa: F401
    from airflow.providers.ssh.operators.ssh import SSHOperator  # noqa: F401
    from airflow.providers.standard.operators.bash import BashOperator  # noqa: F401
    from airflow.providers.standard.operators.empty import EmptyOperator  # noqa: F401
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,  # noqa: F401
        PythonOperator,  # noqa: F401
        ShortCircuitOperator,  # noqa: F401
    )
    from airflow.providers.standard.sensors.bash import BashSensor  # noqa: F401
    from airflow.providers.standard.sensors.python import PythonSensor  # noqa: F401
    from airflow.sdk import get_parsing_context  # noqa: F401
    from airflow.timetables.events import EventsTimetable  # noqa: F401
    from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable  # noqa: F401
    from airflow.timetables.trigger import CronTriggerTimetable, DeltaTriggerTimetable, MultipleCronTriggerTimetable  # noqa: F401
    from airflow.utils.trigger_rule import TriggerRule  # noqa: F401
elif _airflow_3() is False:
    _log.info("Using Airflow 2.x imports")

    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.models.param import Param  # noqa: F401
    from airflow.models.pool import Pool, PoolNotFound  # noqa: F401
    from airflow.models.variable import Variable  # noqa: F401
    from airflow.providers.ssh.hooks.ssh import SSHHook  # noqa: F401  # noqa: F401
    from airflow.providers.ssh.operators.ssh import SSHOperator  # noqa: F401  # noqa: F401
    from airflow.providers.standard.operators.bash import BashOperator  # noqa: F401
    from airflow.providers.standard.operators.empty import EmptyOperator  # noqa: F401
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,  # noqa: F401
        PythonOperator,  # noqa: F401
        ShortCircuitOperator,  # noqa: F401
    )
    from airflow.providers.standard.sensors.bash import BashSensor  # noqa: F401
    from airflow.providers.standard.sensors.python import PythonSensor  # noqa: F401
    from airflow.timetables.events import EventsTimetable  # noqa: F401
    from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable  # noqa: F401
    from airflow.timetables.trigger import CronTriggerTimetable  # noqa: F401

    # NOTE: No MultipleCronTriggerTimetable, DeltaTriggerTimetable
    from airflow.utils.dag_parsing_context import get_parsing_context  # noqa: F401
    from airflow.utils.trigger_rule import TriggerRule  # noqa: F401
else:

    class AirflowFailException(Exception):
        """Exception raised when a task fails in Airflow."""

        pass

    class AirflowSkipException(Exception):
        """Exception raised when a task is skipped in Airflow."""

        pass

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
        def __init__(self, **kwargs):
            self.value = kwargs.get("value", None)
            self.default = kwargs.get("default", None)
            self.title = kwargs.get("title", None)
            self.description = kwargs.get("description", None)

            type = kwargs.get("type", "object")
            if not isinstance(type, list):
                type = [type]

            if self.default is not None and "null" not in type:
                type.append("null")

            self.type = type
            self.schema = kwargs.pop(
                "schema",
                {
                    "value": self.value,
                    "title": self.title,
                    "description": self.description,
                    "type": self.type,
                },
            )

        def serialize(self) -> dict:
            return {"value": self.default, "description": self.description, "schema": self.schema}

    class Pool:
        def __init__(self, pool: str, slots: int = 0, description: str = "", include_deferred: bool = False):
            self.pool = pool
            self.slots = slots
            self.description = description
            self.include_deferred = include_deferred

        @classmethod
        def get_pool(cls, pool_name: str, *args, **kwargs) -> "Pool":
            # Simulate getting a pool from Airflow
            return cls(pool=pool_name, slots=5, description="Test pool")

        @classmethod
        def create_or_update_pool(cls, name: str, slots: int = 0, description: str = "", include_deferred: bool = False, *args, **kwargs):
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

    class _ParsingContext(_AirflowPydanticMarker):
        dag_id = None

    def get_parsing_context():
        # Airflow not installed, so no parsing context
        return _ParsingContext()

    class PythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.PythonOperator"

    class BranchPythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.BranchPythonOperator"

    class ShortCircuitOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.ShortCircuitOperator"

    class BashOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.bash.BashOperator"

    class PythonSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.python.PythonSensor"

    class BashSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.bash.BashSensor"

    class EmptyOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.empty.EmptyOperator"

    class SSHHook(_AirflowPydanticMarker):
        def __init__(self, remote_host: str, username: str = None, password: str = None, key_file: str = None, **kwargs):
            self.remote_host = remote_host
            self.username = username or getuser()
            self.password = password
            self.key_file = key_file
            self.ssh_conn_id = kwargs.pop("ssh_conn_id", None)
            self.port = kwargs.pop("port", 22)
            self.conn_timeout = kwargs.pop("conn_timeout", None)
            self.cmd_timeout = kwargs.pop("cmd_timeout", 10)
            self.keepalive_interval = kwargs.pop("keepalive_interval", 30)
            self.banner_timeout = kwargs.pop("banner_timeout", 30.0)
            self.auth_timeout = kwargs.pop("auth_timeout", None)

    class SSHOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.ssh.operators.ssh.SSHOperator"

    class EventsTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.events.EventsTimetable"

        def __init__(self, event_dates, restrict_to_events: bool = False, presorted: bool = False, description: str = None):
            self.event_dates = event_dates
            self.restrict_to_events = restrict_to_events
            self.presorted = presorted
            self.description = description

    class CronDataIntervalTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.interval.CronDataIntervalTimetable"

        def __init__(self, cron, timezone) -> None:
            self.cron = cron
            self.timezone = timezone

    class DeltaDataIntervalTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.interval.DeltaDataIntervalTimetable"

        def __init__(self, delta) -> None:
            self._delta = delta

    class CronTriggerTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.trigger.CronTriggerTimetable"

        def __init__(self, cron, timezone) -> None:
            self.cron = cron
            self.timezone = timezone


if _airflow_3() in (False, None):

    class DeltaTriggerTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.trigger.DeltaTriggerTimetable"

        def __init__(self, delta, *, interval=timedelta()) -> None:
            self.delta = delta
            self.interval = interval

    class MultipleCronTriggerTimetable(_AirflowPydanticMarker):
        _original = "airflow.timetables.trigger.MultipleCronTriggerTimetable"

        def __init__(self, *crons, timezone, interval=timedelta(), run_immediately=False) -> None:
            self.crons = crons
            self.timezone = timezone
            self.interval = interval
            self.run_immediately = run_immediately
