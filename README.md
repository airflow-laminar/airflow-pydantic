# airflow-pydantic

Pydantic models for Apache Airflow

[![Build Status](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-pydantic/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-pydantic)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-pydantic)](https://github.com/airflow-laminar/airflow-pydantic)
[![PyPI](https://img.shields.io/pypi/v/airflow-pydantic.svg)](https://pypi.python.org/pypi/airflow-pydantic)

## Overview

[Pydantic](https://docs.pydantic.dev/latest/) models of Apache Airflow data structures:

## Core

- [DAG / DAG Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/dag/index.html#airflow.models.dag.DAG)
- [Task / Task Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator)

### Operators/Sensors

- [PythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator)
- [BashOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/bash/index.html#airflow.providers.standard.operators.bash.BashOperator)
- [SSHOperator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html#airflow.providers.ssh.operators.ssh.SSHOperator)
- [BranchPythonOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.BranchPythonOperator)
- [ShortCircuitOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.ShortCircuitOperator)
- [BashSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/bash/index.html#airflow.providers.standard.sensors.bash.BashSensor)
- [PythonSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/python/index.html#airflow.providers.standard.sensors.python.PythonSensor)
- [TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/trigger_dag_run.html)
- EmptyOperator

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

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
