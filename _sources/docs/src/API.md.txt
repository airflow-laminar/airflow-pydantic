# API Reference

## Top Level

```{eval-rst}
.. currentmodule:: airflow_pydantic

.. autosummary::
   :toctree: _build

    DagArgs
    Dag
    TaskArgs
    Task
```

## Operators

```{eval-rst}
.. currentmodule:: airflow_pydantic

.. autosummary::
   :toctree: _build

    ApprovalOperatorArgs
    BashOperatorArgs
    BranchDayOfWeekOperatorArgs
    BranchExternalPythonOperatorArgs
    BranchPythonOperatorArgs
    BranchPythonVirtualenvOperatorArgs
    EmptyOperatorArgs
    ExternalPythonOperatorArgs
    ExternalTaskMarkerArgs
    HITLEntryOperatorArgs
    HITLOperatorArgs
    HITLBranchOperatorArgs
    PythonOperatorArgs
    PythonVirtualenvOperatorArgs
    ShortCircuitOperatorArgs
    SSHOperatorArgs
    TriggerDagRunOperatorArgs

```

## Sensors

```{eval-rst}
.. currentmodule:: airflow_pydantic

.. autosummary::
   :toctree: _build

    BashSensor
    DateTimeSensor
    DateTimeSensorAsync
    DayOfWeekSensor
    ExternalTaskSensor
    FileSensor
    PythonSensor
    TimeSensor
    WaitSensor
```

## Utilities

```{eval-rst}
.. currentmodule:: airflow_pydantic

.. autosummary::
   :toctree: _build

    Param
    Pool
    SSHHook
    Variable

  CronDataIntervalTimetable
  CronTriggerTimetable
  DeltaDataIntervalTimetable
  DeltaTriggerTimetable
  EventsTimetable
  MultipleCronTriggerTimetable

    DatetimeArg
    ScheduleArg
    CallablePath
    ImportPath
```
