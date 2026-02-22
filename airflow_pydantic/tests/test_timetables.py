"""Tests for timetable models."""

from datetime import datetime, timedelta

from airflow_pydantic.utils.timetables import (
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
    EventsTimetable,
    MultipleCronTriggerTimetable,
)


class TestCronTriggerTimetable:
    """Tests for CronTriggerTimetable model."""

    def test_cron_trigger_timetable_basic(self):
        """Test basic CronTriggerTimetable creation."""
        tt = CronTriggerTimetable(cron="0 9 * * *")
        assert tt.cron == "0 9 * * *"

    def test_cron_trigger_timetable_with_timezone(self):
        """Test CronTriggerTimetable with timezone."""
        tt = CronTriggerTimetable(cron="0 9 * * MON-FRI", timezone="America/New_York")
        assert tt.cron == "0 9 * * MON-FRI"
        assert tt.timezone == "America/New_York"

    def test_cron_trigger_timetable_with_interval(self):
        """Test CronTriggerTimetable with interval."""
        tt = CronTriggerTimetable(
            cron="0 0 * * *",
            interval=timedelta(hours=1),
        )
        assert tt.interval == timedelta(hours=1)

    def test_cron_trigger_timetable_run_immediately(self):
        """Test CronTriggerTimetable with run_immediately option."""
        tt = CronTriggerTimetable(
            cron="0 0 * * *",
            run_immediately=True,
        )
        assert tt.run_immediately is True

    def test_cron_trigger_timetable_roundtrip(self):
        """Test CronTriggerTimetable serialization roundtrip."""
        tt = CronTriggerTimetable(cron="0 9 * * *", timezone="UTC")
        dumped = tt.model_dump(exclude_unset=True)
        restored = CronTriggerTimetable.model_validate(dumped)
        assert tt.cron == restored.cron
        assert tt.timezone == restored.timezone


class TestMultipleCronTriggerTimetable:
    """Tests for MultipleCronTriggerTimetable model."""

    def test_multiple_cron_trigger_timetable_basic(self):
        """Test basic MultipleCronTriggerTimetable creation."""
        tt = MultipleCronTriggerTimetable(
            crons=["0 9 * * MON-FRI", "0 12 * * SAT,SUN"],
            timezone="UTC",
        )
        assert len(tt.crons) == 2
        assert tt.timezone == "UTC"

    def test_multiple_cron_trigger_timetable_with_interval(self):
        """Test MultipleCronTriggerTimetable with interval."""
        tt = MultipleCronTriggerTimetable(
            crons=["0 9 * * *", "0 18 * * *"],
            timezone="America/Los_Angeles",
            interval=timedelta(hours=2),
        )
        assert tt.interval == timedelta(hours=2)

    def test_multiple_cron_trigger_timetable_roundtrip(self):
        """Test MultipleCronTriggerTimetable serialization roundtrip."""
        tt = MultipleCronTriggerTimetable(
            crons=["0 9 * * *", "0 18 * * *"],
            timezone="UTC",
        )
        dumped = tt.model_dump(exclude_unset=True)
        restored = MultipleCronTriggerTimetable.model_validate(dumped)
        assert tt.crons == restored.crons
        assert tt.timezone == restored.timezone


class TestCronDataIntervalTimetable:
    """Tests for CronDataIntervalTimetable model."""

    def test_cron_data_interval_timetable_basic(self):
        """Test basic CronDataIntervalTimetable creation."""
        tt = CronDataIntervalTimetable(cron="0 0 * * *")
        assert tt.cron == "0 0 * * *"

    def test_cron_data_interval_timetable_with_timezone(self):
        """Test CronDataIntervalTimetable with timezone."""
        tt = CronDataIntervalTimetable(cron="0 0 * * *", timezone="Europe/London")
        assert tt.timezone == "Europe/London"

    def test_cron_data_interval_timetable_roundtrip(self):
        """Test CronDataIntervalTimetable serialization roundtrip."""
        tt = CronDataIntervalTimetable(cron="0 6 * * *", timezone="UTC")
        dumped = tt.model_dump(exclude_unset=True)
        restored = CronDataIntervalTimetable.model_validate(dumped)
        assert tt.cron == restored.cron


class TestDeltaDataIntervalTimetable:
    """Tests for DeltaDataIntervalTimetable model."""

    def test_delta_data_interval_timetable_with_timedelta(self):
        """Test DeltaDataIntervalTimetable with timedelta."""
        tt = DeltaDataIntervalTimetable(delta=timedelta(hours=6))
        assert tt.delta == timedelta(hours=6)

    def test_delta_data_interval_timetable_roundtrip(self):
        """Test DeltaDataIntervalTimetable serialization roundtrip."""
        tt = DeltaDataIntervalTimetable(delta=timedelta(days=1))
        dumped = tt.model_dump(exclude_unset=True)
        restored = DeltaDataIntervalTimetable.model_validate(dumped)
        assert tt.delta == restored.delta


class TestDeltaTriggerTimetable:
    """Tests for DeltaTriggerTimetable model."""

    def test_delta_trigger_timetable_basic(self):
        """Test basic DeltaTriggerTimetable creation."""
        tt = DeltaTriggerTimetable(delta=timedelta(hours=12))
        assert tt.delta == timedelta(hours=12)

    def test_delta_trigger_timetable_with_interval(self):
        """Test DeltaTriggerTimetable with interval."""
        tt = DeltaTriggerTimetable(
            delta=timedelta(hours=6),
            interval=timedelta(hours=1),
        )
        assert tt.delta == timedelta(hours=6)
        assert tt.interval == timedelta(hours=1)

    def test_delta_trigger_timetable_roundtrip(self):
        """Test DeltaTriggerTimetable serialization roundtrip."""
        tt = DeltaTriggerTimetable(delta=timedelta(hours=4))
        dumped = tt.model_dump(exclude_unset=True)
        restored = DeltaTriggerTimetable.model_validate(dumped)
        assert tt.delta == restored.delta


class TestEventsTimetable:
    """Tests for EventsTimetable model."""

    def test_events_timetable_basic(self):
        """Test basic EventsTimetable creation."""
        events = [
            datetime(2024, 1, 15, 9, 0),  # noqa: DTZ001
            datetime(2024, 2, 15, 9, 0),  # noqa: DTZ001
            datetime(2024, 3, 15, 9, 0),  # noqa: DTZ001
        ]
        tt = EventsTimetable(event_dates=events)
        assert len(tt.event_dates) == 3

    def test_events_timetable_with_options(self):
        """Test EventsTimetable with options."""
        events = [datetime(2024, 6, 1)]  # noqa: DTZ001
        tt = EventsTimetable(
            event_dates=events,
            restrict_to_events=True,
            presorted=True,
            description="Monthly events",
        )
        assert tt.restrict_to_events is True
        assert tt.presorted is True
        assert tt.description == "Monthly events"

    def test_events_timetable_roundtrip(self):
        """Test EventsTimetable serialization roundtrip."""
        events = [datetime(2024, 1, 1), datetime(2024, 7, 1)]  # noqa: DTZ001
        tt = EventsTimetable(event_dates=events)
        dumped = tt.model_dump(exclude_unset=True)
        restored = EventsTimetable.model_validate(dumped)
        assert len(tt.event_dates) == len(restored.event_dates)
