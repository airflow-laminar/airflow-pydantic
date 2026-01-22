"""Tests for airflow.py module - fallback classes and utilities."""

import pytest

from airflow_pydantic.airflow import (
    AirflowFailException,
    AirflowSkipException,
    TriggerRule,
    _AirflowPydanticMarker,
)


class TestAirflowPydanticMarker:
    """Tests for _AirflowPydanticMarker base class."""

    def test_marker_class_exists(self):
        """Test that marker class exists."""
        assert _AirflowPydanticMarker is not None

    def test_marker_is_class(self):
        """Test that marker is a class."""
        assert isinstance(_AirflowPydanticMarker, type)


class TestAirflowExceptions:
    """Tests for Airflow exception classes."""

    def test_airflow_fail_exception(self):
        """Test AirflowFailException can be raised and caught."""
        with pytest.raises(AirflowFailException):
            raise AirflowFailException("Task failed")

    def test_airflow_skip_exception(self):
        """Test AirflowSkipException can be raised and caught."""
        with pytest.raises(AirflowSkipException):
            raise AirflowSkipException("Task skipped")

    def test_airflow_fail_exception_message(self):
        """Test AirflowFailException preserves message."""
        try:
            raise AirflowFailException("Custom message")
        except AirflowFailException as e:
            assert "Custom message" in str(e)

    def test_airflow_skip_exception_message(self):
        """Test AirflowSkipException preserves message."""
        try:
            raise AirflowSkipException("Skip reason")
        except AirflowSkipException as e:
            assert "Skip reason" in str(e)


class TestTriggerRule:
    """Tests for TriggerRule enum."""

    def test_trigger_rule_values(self):
        """Test TriggerRule enum values exist."""
        assert TriggerRule.ALL_SUCCESS == "all_success"
        assert TriggerRule.ALL_FAILED == "all_failed"
        assert TriggerRule.ALL_DONE == "all_done"
        assert TriggerRule.ONE_SUCCESS == "one_success"
        assert TriggerRule.ONE_FAILED == "one_failed"
        assert TriggerRule.NONE_FAILED == "none_failed"
        assert TriggerRule.ALWAYS == "always"

    def test_trigger_rule_str(self):
        """Test TriggerRule string representation."""
        assert str(TriggerRule.ALL_SUCCESS) == "all_success"
        assert str(TriggerRule.ALWAYS) == "always"

    def test_trigger_rule_is_valid(self):
        """Test TriggerRule.is_valid method."""
        assert TriggerRule.is_valid("all_success")
        assert TriggerRule.is_valid("always")
        assert not TriggerRule.is_valid("invalid_trigger")

    def test_trigger_rule_all_triggers(self):
        """Test TriggerRule.all_triggers method."""
        all_triggers = TriggerRule.all_triggers()
        assert isinstance(all_triggers, set)
        assert TriggerRule.ALL_SUCCESS in all_triggers
        assert TriggerRule.ALWAYS in all_triggers


class TestPoolClass:
    """Tests for Pool class."""

    def test_pool_class_exists(self):
        """Test Pool class exists."""
        from airflow_pydantic.airflow import Pool

        assert Pool is not None

    def test_pool_has_get_pool(self):
        """Test Pool has get_pool method."""
        from airflow_pydantic.airflow import Pool

        assert hasattr(Pool, "get_pool") or hasattr(Pool, "get")


class TestVariableClass:
    """Tests for Variable class."""

    def test_variable_class_exists(self):
        """Test Variable class exists."""
        from airflow_pydantic.airflow import Variable

        assert Variable is not None

    def test_variable_has_get(self):
        """Test Variable has get method."""
        from airflow_pydantic.airflow import Variable

        assert hasattr(Variable, "get")


class TestParamClass:
    """Tests for Param class."""

    def test_param_class_exists(self):
        """Test Param class exists."""
        from airflow_pydantic.airflow import Param

        assert Param is not None


class TestDAGClass:
    """Tests for DAG class."""

    def test_dag_class_exists(self):
        """Test DAG class exists."""
        from airflow_pydantic.airflow import DAG

        assert DAG is not None


class TestSSHHookClass:
    """Tests for SSHHook class."""

    def test_ssh_hook_class_exists(self):
        """Test SSHHook class exists."""
        from airflow_pydantic.airflow import SSHHook

        assert SSHHook is not None


class TestOperatorClasses:
    """Tests for operator classes."""

    def test_bash_operator_exists(self):
        """Test BashOperator exists."""
        from airflow_pydantic.airflow import BashOperator

        assert BashOperator is not None

    def test_python_operator_exists(self):
        """Test PythonOperator exists."""
        from airflow_pydantic.airflow import PythonOperator

        assert PythonOperator is not None

    def test_empty_operator_exists(self):
        """Test EmptyOperator exists."""
        from airflow_pydantic.airflow import EmptyOperator

        assert EmptyOperator is not None

    def test_ssh_operator_exists(self):
        """Test SSHOperator exists."""
        from airflow_pydantic.airflow import SSHOperator

        assert SSHOperator is not None

    def test_trigger_dagrun_operator_exists(self):
        """Test TriggerDagRunOperator exists."""
        from airflow_pydantic.airflow import TriggerDagRunOperator

        assert TriggerDagRunOperator is not None

    def test_branch_python_operator_exists(self):
        """Test BranchPythonOperator exists."""
        from airflow_pydantic.airflow import BranchPythonOperator

        assert BranchPythonOperator is not None

    def test_short_circuit_operator_exists(self):
        """Test ShortCircuitOperator exists."""
        from airflow_pydantic.airflow import ShortCircuitOperator

        assert ShortCircuitOperator is not None


class TestSensorClasses:
    """Tests for sensor classes."""

    def test_bash_sensor_exists(self):
        """Test BashSensor exists."""
        from airflow_pydantic.airflow import BashSensor

        assert BashSensor is not None

    def test_python_sensor_exists(self):
        """Test PythonSensor exists."""
        from airflow_pydantic.airflow import PythonSensor

        assert PythonSensor is not None

    def test_datetime_sensor_exists(self):
        """Test DateTimeSensor exists."""
        from airflow_pydantic.airflow import DateTimeSensor

        assert DateTimeSensor is not None

    def test_time_sensor_exists(self):
        """Test TimeSensor exists."""
        from airflow_pydantic.airflow import TimeSensor

        assert TimeSensor is not None

    def test_file_sensor_exists(self):
        """Test FileSensor exists."""
        from airflow_pydantic.airflow import FileSensor

        assert FileSensor is not None

    def test_external_task_sensor_exists(self):
        """Test ExternalTaskSensor exists."""
        from airflow_pydantic.airflow import ExternalTaskSensor

        assert ExternalTaskSensor is not None

    def test_wait_sensor_exists(self):
        """Test WaitSensor exists."""
        from airflow_pydantic.airflow import WaitSensor

        assert WaitSensor is not None


class TestTimetableClasses:
    """Tests for timetable classes."""

    def test_events_timetable_exists(self):
        """Test EventsTimetable exists."""
        from airflow_pydantic.airflow import EventsTimetable

        assert EventsTimetable is not None

    def test_cron_data_interval_timetable_exists(self):
        """Test CronDataIntervalTimetable exists."""
        from airflow_pydantic.airflow import CronDataIntervalTimetable

        assert CronDataIntervalTimetable is not None

    def test_delta_data_interval_timetable_exists(self):
        """Test DeltaDataIntervalTimetable exists."""
        from airflow_pydantic.airflow import DeltaDataIntervalTimetable

        assert DeltaDataIntervalTimetable is not None

    def test_cron_trigger_timetable_exists(self):
        """Test CronTriggerTimetable exists."""
        from airflow_pydantic.airflow import CronTriggerTimetable

        assert CronTriggerTimetable is not None

    def test_delta_trigger_timetable_exists(self):
        """Test DeltaTriggerTimetable exists."""
        from airflow_pydantic.airflow import DeltaTriggerTimetable

        assert DeltaTriggerTimetable is not None

    def test_multiple_cron_trigger_timetable_exists(self):
        """Test MultipleCronTriggerTimetable exists."""
        from airflow_pydantic.airflow import MultipleCronTriggerTimetable

        assert MultipleCronTriggerTimetable is not None


class TestModuleFunctions:
    """Tests for module-level functions."""

    def test_get_pool_function_exists(self):
        """Test get_pool function exists."""
        from airflow_pydantic.airflow import get_pool

        assert callable(get_pool)

    def test_create_or_update_pool_function_exists(self):
        """Test create_or_update_pool function exists."""
        from airflow_pydantic.airflow import create_or_update_pool

        assert callable(create_or_update_pool)

    def test_get_parsing_context_exists(self):
        """Test get_parsing_context function exists."""
        from airflow_pydantic.airflow import get_parsing_context

        assert callable(get_parsing_context)

    def test_provide_session_exists(self):
        """Test provide_session exists."""
        from airflow_pydantic.airflow import provide_session

        assert provide_session is not None


class TestAllExports:
    """Tests for __all__ exports."""

    def test_all_exports_exist(self):
        """Test all items in __all__ can be imported."""
        from airflow_pydantic import airflow

        for name in airflow.__all__:
            assert hasattr(airflow, name), f"Missing export: {name}"

    def test_trigger_rule_in_all(self):
        """Test TriggerRule is in __all__."""
        from airflow_pydantic import airflow

        assert "TriggerRule" in airflow.__all__

    def test_dag_in_all(self):
        """Test DAG is in __all__."""
        from airflow_pydantic import airflow

        assert "DAG" in airflow.__all__

    def test_pool_in_all(self):
        """Test Pool is in __all__."""
        from airflow_pydantic import airflow

        assert "Pool" in airflow.__all__

    def test_variable_in_all(self):
        """Test Variable is in __all__."""
        from airflow_pydantic import airflow

        assert "Variable" in airflow.__all__
