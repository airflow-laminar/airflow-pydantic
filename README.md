# airflow-pydantic

Pydantic models for Apache Airflow

[![Build Status](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-pydantic/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-pydantic)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-pydantic)](https://github.com/airflow-laminar/airflow-pydantic)
[![PyPI](https://img.shields.io/pypi/v/airflow-pydantic.svg)](https://pypi.python.org/pypi/airflow-pydantic)

## Overview

[Pydantic](https://docs.pydantic.dev/latest/) models of Apache Airflow data structures.

## Core

- [DAG / DAG Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/dag/index.html#airflow.models.dag.DAG)
- [Task / Task Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator)

### Operators

- [ApprovalOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html#airflow.providers.standard.operators.hitl.HITLOperator)
- [BashOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html#airflow.providers.standard.operators.bash.BashOperator)
- [BranchDayOfWeekOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/weekday/index.html#airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator)
- [BranchExternalPythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.BranchExternalPythonOperator)
- [BranchPythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.BranchPythonOperator)
- [BranchPythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonVirtualenvOperator)
- [EmptyOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/empty/index.html#airflow.providers.standard.operators.empty.EmptyOperator)
- [ExternalPythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.ExternalPythonOperator)
- [ExternalTaskMarker](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/external_task/index.html#airflow.providers.standard.sensors.external_task.ExternalTaskMarker)
- [HITLEntryOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html#airflow.providers.standard.operators.hitl.HITLEntryOperator)
- [HITLOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html#airflow.providers.standard.operators.hitl.HITLOperator)
- [HITLBranchOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html#airflow.providers.standard.operators.hitl.HITLOperator)
- [PythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator)
- [PythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonVirtualenvOperator)
- [ShortCircuitOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.ShortCircuitOperator)
- [SSHOperator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html#airflow.providers.ssh.operators.ssh.SSHOperator)
- [TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/trigger_dag_run.html)

### Sensors

- [BashSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/bash/index.html#airflow.providers.standard.sensors.bash.BashSensor)
- [DateTimeSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/date_time/index.html#airflow.providers.standard.sensors.date_time.DateTimeSensor)
- [DateTimeSensorAsync](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/date_time/index.html#airflow.providers.standard.sensors.date_time.DateTimeSensorAsync)
- [DayOfWeekSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/weekday/index.html#airflow.providers.standard.sensors.weekday.DayOfWeekSensor)
- [ExternalTaskSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/external_task/index.html#airflow.providers.standard.sensors.external_task.ExternalTaskSensor)
- [FileSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/filesystem/index.html#airflow.providers.standard.sensors.filesystem.FileSensor)
- [PythonSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/python/index.html#airflow.providers.standard.sensors.python.PythonSensor)
- [TimeSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/time/index.html#airflow.providers.standard.sensors.time.TimeSensor)
- [WaitSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/time_delta/index.html#airflow.providers.standard.sensors.time_delta.WaitSensor)

### Other

- [Param](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html)
- [Pool](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [SSHHook](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/hooks/ssh/index.html#airflow.providers.ssh.hooks.ssh.SSHHook)
- [Variable](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
- [CronDataIntervalTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/interval/index.html#airflow.timetables.interval.CronDataIntervalTimetable)
- [CronTriggerTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/trigger/index.html#airflow.timetables.trigger.CronTriggerTimetable)
- [DeltaDataIntervalTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/interval/index.html#airflow.timetables.interval.DeltaDataIntervalTimetable)
- [DeltaTriggerTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/trigger/index.html#airflow.timetables.trigger.DeltaTriggerTimetable)
- [EventsTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/events/index.html#airflow.timetables.events.EventsTimetable)
- [MultipleCronTriggerTimetable](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/trigger/index.html#airflow.timetables.trigger.MultipleCronTriggerTimetable)

## Usage

All operators and sensors support two methods:

- `instantiate()`: Create a concrete Airflow instance
- `render()`: Return Python code to instantiate the instance, for use in [airflow-config](https://github.com/airflow-laminar/airflow-config)

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
