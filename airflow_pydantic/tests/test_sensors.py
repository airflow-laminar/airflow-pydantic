"""Tests for sensor models."""

from datetime import datetime, time, timedelta

from airflow_pydantic.sensors.base import BaseSensorArgs
from airflow_pydantic.sensors.datetime import DateTimeSensorArgs, DateTimeSensorAsyncArgs
from airflow_pydantic.sensors.filesystem import FileSensorArgs
from airflow_pydantic.sensors.time import TimeSensorArgs
from airflow_pydantic.sensors.timedelta import TimeDeltaSensorArgs, WaitSensorArgs


class TestBaseSensorArgs:
    """Tests for BaseSensorArgs model."""

    def test_base_sensor_args_creation(self):
        """Test basic BaseSensorArgs creation."""
        args = BaseSensorArgs(
            poke_interval=60.0,
            timeout=3600.0,
            mode="poke",
        )
        assert args.poke_interval == 60.0
        assert args.timeout == 3600.0
        assert args.mode == "poke"

    def test_base_sensor_args_with_timedelta(self):
        """Test BaseSensorArgs with timedelta values."""
        args = BaseSensorArgs(
            poke_interval=timedelta(minutes=1),
            timeout=timedelta(hours=1),
        )
        assert args.poke_interval == timedelta(minutes=1)
        assert args.timeout == timedelta(hours=1)

    def test_base_sensor_args_reschedule_mode(self):
        """Test BaseSensorArgs with reschedule mode."""
        args = BaseSensorArgs(mode="reschedule")
        assert args.mode == "reschedule"

    def test_base_sensor_args_soft_fail(self):
        """Test BaseSensorArgs with soft_fail option."""
        args = BaseSensorArgs(soft_fail=True)
        assert args.soft_fail is True

    def test_base_sensor_args_exponential_backoff(self):
        """Test BaseSensorArgs with exponential_backoff."""
        args = BaseSensorArgs(
            exponential_backoff=True,
            max_wait=timedelta(hours=2),
        )
        assert args.exponential_backoff is True
        assert args.max_wait == timedelta(hours=2)

    def test_base_sensor_args_roundtrip(self):
        """Test BaseSensorArgs serialization roundtrip."""
        args = BaseSensorArgs(
            poke_interval=60.0,
            timeout=3600.0,
            mode="poke",
            soft_fail=True,
        )
        dumped = args.model_dump(exclude_unset=True)
        restored = BaseSensorArgs.model_validate(dumped)
        assert args.poke_interval == restored.poke_interval
        assert args.timeout == restored.timeout
        assert args.mode == restored.mode


class TestTimeSensorArgs:
    """Tests for TimeSensorArgs model."""

    def test_time_sensor_args_creation(self):
        """Test basic TimeSensorArgs creation."""
        args = TimeSensorArgs(target_time=time(12, 0, 0))
        assert args.target_time == time(12, 0, 0)

    def test_time_sensor_args_with_deferrable(self):
        """Test TimeSensorArgs with deferrable option."""
        args = TimeSensorArgs(target_time=time(9, 30), deferrable=True)
        assert args.target_time == time(9, 30)
        assert args.deferrable is True

    def test_time_sensor_args_roundtrip(self):
        """Test TimeSensorArgs serialization roundtrip."""
        args = TimeSensorArgs(target_time=time(15, 45, 30))
        dumped = args.model_dump(exclude_unset=True)
        restored = TimeSensorArgs.model_validate(dumped)
        assert args.target_time == restored.target_time


class TestDateTimeSensorArgs:
    """Tests for DateTimeSensorArgs model."""

    def test_datetime_sensor_args_creation(self):
        """Test basic DateTimeSensorArgs creation."""
        target = datetime(2024, 6, 15, 12, 0, 0)  # noqa: DTZ001
        args = DateTimeSensorArgs(target_time=target)
        assert args.target_time == target

    def test_datetime_sensor_args_with_string(self):
        """Test DateTimeSensorArgs with string datetime."""
        args = DateTimeSensorArgs(target_time="2024-06-15T12:00:00")
        assert args.target_time == datetime(2024, 6, 15, 12, 0, 0)  # noqa: DTZ001

    def test_datetime_sensor_args_roundtrip(self):
        """Test DateTimeSensorArgs serialization roundtrip."""
        target = datetime(2024, 6, 15, 12, 0, 0)  # noqa: DTZ001
        args = DateTimeSensorArgs(target_time=target)
        dumped = args.model_dump(exclude_unset=True)
        restored = DateTimeSensorArgs.model_validate(dumped)
        assert args.target_time == restored.target_time


class TestDateTimeSensorAsyncArgs:
    """Tests for DateTimeSensorAsyncArgs model."""

    def test_datetime_sensor_async_args_creation(self):
        """Test basic DateTimeSensorAsyncArgs creation."""
        target = datetime(2024, 6, 15, 12, 0, 0)  # noqa: DTZ001
        args = DateTimeSensorAsyncArgs(target_time=target)
        assert args.target_time == target

    def test_datetime_sensor_async_args_with_triggers(self):
        """Test DateTimeSensorAsyncArgs with trigger options."""
        args = DateTimeSensorAsyncArgs(
            target_time=datetime(2024, 6, 15, 12, 0, 0),  # noqa: DTZ001
            start_from_trigger=True,
            end_from_trigger=True,
            trigger_kwargs={"some_arg": "value"},
        )
        assert args.start_from_trigger is True
        assert args.end_from_trigger is True
        assert args.trigger_kwargs == {"some_arg": "value"}


class TestTimeDeltaSensorArgs:
    """Tests for TimeDeltaSensorArgs model."""

    def test_timedelta_sensor_args_creation(self):
        """Test basic TimeDeltaSensorArgs creation."""
        args = TimeDeltaSensorArgs(delta=timedelta(hours=1))
        assert args.delta == timedelta(hours=1)

    def test_timedelta_sensor_args_with_deferrable(self):
        """Test TimeDeltaSensorArgs with deferrable option."""
        args = TimeDeltaSensorArgs(delta=timedelta(minutes=30), deferrable=True)
        assert args.delta == timedelta(minutes=30)
        assert args.deferrable is True

    def test_timedelta_sensor_args_roundtrip(self):
        """Test TimeDeltaSensorArgs serialization roundtrip."""
        args = TimeDeltaSensorArgs(delta=timedelta(hours=2, minutes=15))
        dumped = args.model_dump(exclude_unset=True)
        restored = TimeDeltaSensorArgs.model_validate(dumped)
        assert args.delta == restored.delta


class TestWaitSensorArgs:
    """Tests for WaitSensorArgs model."""

    def test_wait_sensor_args_with_timedelta(self):
        """Test WaitSensorArgs with timedelta."""
        args = WaitSensorArgs(time_to_wait=timedelta(minutes=10))
        assert args.time_to_wait == timedelta(minutes=10)

    def test_wait_sensor_args_with_int(self):
        """Test WaitSensorArgs with int (seconds)."""
        args = WaitSensorArgs(time_to_wait=600)
        assert args.time_to_wait == 600

    def test_wait_sensor_args_with_deferrable(self):
        """Test WaitSensorArgs with deferrable option."""
        args = WaitSensorArgs(time_to_wait=timedelta(minutes=5), deferrable=True)
        assert args.deferrable is True

    def test_wait_sensor_args_roundtrip(self):
        """Test WaitSensorArgs serialization roundtrip."""
        args = WaitSensorArgs(time_to_wait=timedelta(hours=1))
        dumped = args.model_dump(exclude_unset=True)
        restored = WaitSensorArgs.model_validate(dumped)
        assert args.time_to_wait == restored.time_to_wait


class TestFileSensorArgs:
    """Tests for FileSensorArgs model."""

    def test_file_sensor_args_creation(self):
        """Test basic FileSensorArgs creation."""
        args = FileSensorArgs(filepath="/path/to/file.txt")
        assert args.filepath == "/path/to/file.txt"

    def test_file_sensor_args_with_connection(self):
        """Test FileSensorArgs with connection ID."""
        args = FileSensorArgs(
            filepath="/data/file.csv",
            fs_conn_id="my_fs_conn",
        )
        assert args.fs_conn_id == "my_fs_conn"
        assert args.filepath == "/data/file.csv"

    def test_file_sensor_args_recursive(self):
        """Test FileSensorArgs with recursive option."""
        args = FileSensorArgs(
            filepath="/data/**/*.csv",
            recursive=True,
        )
        assert args.recursive is True

    def test_file_sensor_args_deferrable(self):
        """Test FileSensorArgs with deferrable mode."""
        args = FileSensorArgs(
            filepath="/data/file.txt",
            deferrable=True,
            start_from_trigger=True,
            trigger_kwargs={"poll_interval": 5},
        )
        assert args.deferrable is True
        assert args.start_from_trigger is True
        assert args.trigger_kwargs == {"poll_interval": 5}

    def test_file_sensor_args_roundtrip(self):
        """Test FileSensorArgs serialization roundtrip."""
        args = FileSensorArgs(
            filepath="/data/file.txt",
            fs_conn_id="my_conn",
            recursive=True,
        )
        dumped = args.model_dump(exclude_unset=True)
        restored = FileSensorArgs.model_validate(dumped)
        assert args.filepath == restored.filepath
        assert args.fs_conn_id == restored.fs_conn_id
        assert args.recursive == restored.recursive
