import json
import os
import subprocess
import tempfile
from datetime import timedelta
from enum import Enum
from getpass import getuser
from importlib.metadata import version
from importlib.util import find_spec
from logging import getLogger
from typing import Any

from .migration import _airflow_3

__all__ = [
    "DAG",
    "NEW_SESSION",
    "AirflowFailException",
    "AirflowSkipException",
    "BashOperator",
    "BashSensor",
    "BranchDateTimeOperator",
    "BranchDayOfWeekOperator",
    "BranchExternalPythonOperator",
    "BranchPythonOperator",
    "BranchPythonVirtualenvOperator",
    "CronDataIntervalTimetable",
    "CronTriggerTimetable",
    "DateTimeSensor",
    "DateTimeSensorAsync",
    "DayOfWeekSensor",
    "DeltaDataIntervalTimetable",
    "DeltaTriggerTimetable",
    "EmptyOperator",
    "EventsTimetable",
    "ExternalPythonOperator",
    "ExternalTaskSensor",
    "FileSensor",
    "MultipleCronTriggerTimetable",
    "Param",
    "Pool",
    "PoolNotFound",
    "PythonOperator",
    "PythonSensor",
    "PythonVirtualenvOperator",
    "SSHHook",
    "SSHOperator",
    "ShortCircuitOperator",
    "TimeDeltaSensor",
    "TimeSensor",
    "TriggerDagRunOperator",
    "TriggerRule",
    "Variable",
    "WaitSensor",
    "_AirflowPydanticMarker",
    "get_parsing_context",
    "provide_session",
]

_log = getLogger(__name__)


class _AirflowPydanticMarker: ...


if _airflow_3():
    _log.info("Using Airflow 3.x imports")
    from airflow.api.client import get_current_api_client
    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.models.dag import DAG
    from airflow.models.param import Param
    from airflow.models.pool import Pool, PoolNotFound
    from airflow.models.variable import Variable
    from airflow.providers.ssh.hooks.ssh import SSHHook
    from airflow.providers.ssh.operators.ssh import SSHOperator
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
    from airflow.providers.standard.operators.empty import EmptyOperator

    if (find_spec("apache-airflow") or find_spec("airflow")) and version("apache-airflow") >= "3.0.0":
        from airflow.providers.standard.operators.hitl import (
            ApprovalOperator,
            HITLBranchOperator,
            HITLOperator,
        )

        # NOTE: Airflow 3 Only
        __all__.extend(
            [
                "ApprovalOperator",
                "HITLBranchOperator",
                "HITLOperator",
            ]
        )

    from airflow.providers.standard.operators.python import (
        BranchExternalPythonOperator,
        BranchPythonOperator,
        BranchPythonVirtualenvOperator,
        ExternalPythonOperator,
        PythonOperator,
        PythonVirtualenvOperator,
        ShortCircuitOperator,
    )
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator
    from airflow.providers.standard.sensors.bash import BashSensor
    from airflow.providers.standard.sensors.date_time import DateTimeSensor, DateTimeSensorAsync
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
    from airflow.providers.standard.sensors.filesystem import FileSensor
    from airflow.providers.standard.sensors.python import PythonSensor
    from airflow.providers.standard.sensors.time import TimeSensor
    from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor, WaitSensor
    from airflow.providers.standard.sensors.weekday import DayOfWeekSensor
    from airflow.sdk import get_parsing_context
    from airflow.timetables.events import EventsTimetable
    from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
    from airflow.timetables.trigger import CronTriggerTimetable, DeltaTriggerTimetable, MultipleCronTriggerTimetable
    from airflow.utils.session import NEW_SESSION, provide_session
    from airflow.utils.trigger_rule import TriggerRule

    def _is_database_available() -> bool:
        """
        Check if database access is available.

        In Airflow 3, DAG parsing happens in a separate process without database access.
        This function checks multiple indicators:
        1. Process context - if we're in a "client" context (parsing), DB isn't available
        2. Engine availability - check if SQLAlchemy engine is configured
        3. Actual connection test - try to execute a simple query
        """
        # Check if we're in a DAG parsing (client) context
        # In Airflow 3, DAG parsing sets _AIRFLOW_PROCESS_CONTEXT=client
        process_context = os.environ.get("_AIRFLOW_PROCESS_CONTEXT", "")
        if process_context == "client":
            return False

        try:
            from airflow import settings

            # Check if engine exists
            if settings.engine is None:
                return False

            # Check if Session is configured
            if settings.Session is None:
                return False

            # Try to actually use the session to verify DB is accessible
            from sqlalchemy import text

            with settings.Session() as session:
                session.execute(text("SELECT 1"))
            return True  # noqa: TRY300
        except Exception:  # noqa: BLE001
            return False

    def _get_pool_via_cli(pool_name: str) -> Pool | None:
        """Get pool information using the Airflow CLI."""
        try:
            result = subprocess.run(
                ["airflow", "pools", "get", pool_name, "--output", "json"],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                pools = json.loads(result.stdout)
                if pools and len(pools) > 0:
                    p = pools[0]
                    return Pool(
                        pool=p.get("pool", pool_name),
                        slots=p.get("slots", 0),
                        description=p.get("description", ""),
                        include_deferred=p.get("include_deferred", False),
                    )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, json.JSONDecodeError) as e:
            _log.debug(f"CLI get_pool failed for {pool_name}: {e}")
        except FileNotFoundError:
            _log.debug("Airflow CLI not found in PATH")
        return None

    def _create_pool_via_cli(name: str, slots: int, description: str, include_deferred: bool) -> bool:
        """Create or update a pool using the Airflow CLI."""
        try:
            cmd = ["airflow", "pools", "set", name, str(slots), description or ""]
            if include_deferred:
                cmd.append("--include-deferred")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode == 0:
                _log.debug(f"Created pool {name} via CLI")
                return True
            else:
                _log.debug(f"CLI create_pool failed for {name}: {result.stderr}")
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            _log.debug(f"CLI create_pool failed for {name}: {e}")
        except FileNotFoundError:
            _log.debug("Airflow CLI not found in PATH")
        return False

    def _get_pool_via_airflowctl(pool_name: str) -> Pool | None:
        """Get pool information using airflowctl."""
        try:
            result = subprocess.run(
                ["airflowctl", "pools", "get", pool_name, "--output", "json"],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                pools = json.loads(result.stdout)
                if pools and len(pools) > 0:
                    p = pools[0]
                    return Pool(
                        pool=p.get("name", pool_name),
                        slots=p.get("slots", 0),
                        description=p.get("description", ""),
                        include_deferred=p.get("include_deferred", False),
                    )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, json.JSONDecodeError) as e:
            _log.debug(f"airflowctl get_pool failed for {pool_name}: {e}")
        except FileNotFoundError:
            _log.debug("airflowctl not found in PATH")
        return None

    def _create_pool_via_airflowctl(name: str, slots: int, description: str, include_deferred: bool) -> bool:
        """Create or update a pool using airflowctl via JSON import."""
        try:
            # airflowctl uses a different format - list of pool objects for import
            pool_data = [
                {
                    "name": name,
                    "slots": slots,
                    "description": description or "",
                    "include_deferred": include_deferred,
                }
            ]

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(pool_data, f)
                temp_path = f.name

            try:
                result = subprocess.run(
                    ["airflowctl", "pools", "import", temp_path],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                if result.returncode == 0:
                    _log.debug(f"Created pool {name} via airflowctl")
                    return True
                else:
                    _log.debug(f"airflowctl create_pool failed for {name}: {result.stderr}")
            finally:
                os.unlink(temp_path)
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            _log.debug(f"airflowctl create_pool failed for {name}: {e}")
        except FileNotFoundError:
            _log.debug("airflowctl not found in PATH")
        return False

    def get_pool(pool_name: str, *args, **kwargs) -> Pool:
        """
        Get a pool by name.

        Tries multiple methods:
        1. Direct database access via API client (if available)
        2. Airflow CLI
        3. airflowctl
        """
        # Try direct database access first
        if _is_database_available():
            try:
                client = get_current_api_client()
                ret = client.get_pool(name=pool_name)
                return Pool(
                    pool=ret[0],
                    slots=ret[1],
                    description=ret[2],
                    include_deferred=ret[3],
                )
            except PoolNotFound:
                raise
            except Exception as e:  # noqa: BLE001
                _log.debug(f"Direct get_pool failed for {pool_name}: {e}")

        # Try CLI fallback
        pool = _get_pool_via_cli(pool_name)
        if pool:
            return pool

        # Try airflowctl fallback
        pool = _get_pool_via_airflowctl(pool_name)
        if pool:
            return pool

        raise PoolNotFound(f"Pool {pool_name} not found")

    def create_or_update_pool(name: str, slots: int = 0, description: str = "", include_deferred: bool = False, *args, **kwargs):
        """
        Create or update a pool.

        Tries multiple methods:
        1. Direct database access via API client (if available)
        2. Airflow CLI
        3. airflowctl
        """
        # Try direct database access first
        if _is_database_available():
            try:
                client = get_current_api_client()
                client.create_pool(name=name, slots=slots, description=description, include_deferred=include_deferred)
                return get_pool(name)
            except Exception as e:  # noqa: BLE001
                _log.debug(f"Direct create_pool failed for {name}: {e}")

        # Try CLI fallback
        if _create_pool_via_cli(name, slots, description, include_deferred):
            try:
                return get_pool(name)
            except PoolNotFound:
                return None

        # Try airflowctl fallback
        if _create_pool_via_airflowctl(name, slots, description, include_deferred):
            try:
                return get_pool(name)
            except PoolNotFound:
                return None

        _log.warning(f"Could not create pool {name} - no available method succeeded")
        return None

elif _airflow_3() is False:
    _log.info("Using Airflow 2.x imports")

    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.models.dag import DAG
    from airflow.models.param import Param
    from airflow.models.pool import Pool, PoolNotFound
    from airflow.models.variable import Variable
    from airflow.providers.ssh.hooks.ssh import SSHHook
    from airflow.providers.ssh.operators.ssh import SSHOperator
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import (
        BranchExternalPythonOperator,
        BranchPythonOperator,
        BranchPythonVirtualenvOperator,
        ExternalPythonOperator,
        PythonOperator,
        PythonVirtualenvOperator,
        ShortCircuitOperator,
    )
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator
    from airflow.providers.standard.sensors.bash import BashSensor
    from airflow.providers.standard.sensors.date_time import DateTimeSensor, DateTimeSensorAsync
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
    from airflow.providers.standard.sensors.filesystem import FileSensor
    from airflow.providers.standard.sensors.python import PythonSensor
    from airflow.providers.standard.sensors.time import TimeSensor
    from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor, WaitSensor
    from airflow.providers.standard.sensors.weekday import DayOfWeekSensor
    from airflow.timetables.events import EventsTimetable
    from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
    from airflow.timetables.trigger import CronTriggerTimetable

    # NOTE: No MultipleCronTriggerTimetable, DeltaTriggerTimetable
    from airflow.utils.dag_parsing_context import get_parsing_context
    from airflow.utils.session import NEW_SESSION, provide_session
    from airflow.utils.trigger_rule import TriggerRule

    def create_or_update_pool(name: str, slots: int = 0, description: str = "", include_deferred: bool = False, *args, **kwargs):
        Pool.create_or_update_pool(name, slots, description, include_deferred, *args, **kwargs)

    def get_pool(pool_name: str, *args, **kwargs) -> Pool:
        return Pool.get_pool(pool_name, *args, **kwargs)

else:
    # NOTE: Airflow 3 Only
    __all__.extend(
        [
            "ApprovalOperator",
            "HITLBranchOperator",
            "HITLOperator",
        ]
    )

    class DAG(_AirflowPydanticMarker):
        def __init__(self, **kwargs):
            self.dag_id = kwargs.get("dag_id", "default_dag_id")
            self.default_args = kwargs.get("default_args", {})

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class AirflowFailException(Exception):
        """Exception raised when a task fails in Airflow."""

    class AirflowSkipException(Exception):
        """Exception raised when a task is skipped in Airflow."""

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
        def all_triggers(cls) -> set[str]:
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

    def create_or_update_pool(name: str, slots: int = 0, description: str = "", include_deferred: bool = False, *args, **kwargs):
        # Simulate creating or updating a pool in Airflow
        pass

    def get_pool(pool_name: str, *args, **kwargs) -> Pool:
        # Simulate getting a pool from Airflow
        return Pool(pool=pool_name, slots=5, description="Test pool")

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

    class BashOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.bash.BashOperator"

    class BranchDateTimeOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.datetime.BranchDateTimeOperator"

    class BranchExternalPythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.BranchExternalPythonOperator"

    class BranchPythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.BranchPythonOperator"

    class BranchPythonVirtualenvOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.BranchPythonVirtualenvOperator"

    class EmptyOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.empty.EmptyOperator"

    class ExternalPythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.ExternalPythonOperator"

    class PythonOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.PythonOperator"

    class PythonVirtualenvOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.PythonVirtualenvOperator"

    class ShortCircuitOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.python.ShortCircuitOperator"

    class TriggerDagRunOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator"

    class BranchDayOfWeekOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator"

    class BashSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.bash.BashSensor"

    class DateTimeSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.date_time.DateTimeSensor"

    class DateTimeSensorAsync(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.date_time.DateTimeSensorAsync"

    class DayOfWeekSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.weekday.DayOfWeekSensor"

    class ExternalTaskSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.external_task.ExternalTaskSensor"

    class FileSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.filesystem.FileSensor"

    class PythonSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.python.PythonSensor"

    class TimeSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.time.TimeSensor"

    class TimeDeltaSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.time_delta.TimeDeltaSensor"

    class WaitSensor(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.sensors.time_delta.WaitSensor"

    class SSHHook(_AirflowPydanticMarker):
        def __init__(self, remote_host: str, username: str | None = None, password: str | None = None, key_file: str | None = None, **kwargs):
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

        def __init__(self, event_dates, restrict_to_events: bool = False, presorted: bool = False, description: str | None = None):
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

    NEW_SESSION = Any
    provide_session = lambda f: f


if _airflow_3() in (False, None):

    class ApprovalOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.hitl.ApprovalOperator"

    class HITLOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.hitl.HITLOperator"

    class HITLBranchOperator(_AirflowPydanticMarker):
        _original = "airflow.providers.standard.operators.hitl.HITLBranchOperator"

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
