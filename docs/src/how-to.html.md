# How-to guides

These guides cover common model construction, rendering, scheduling, resource,
and serialization tasks.

## How to build a DAG in Python

Construct operator-specific task models and place them in a `Dag`:

```python
from airflow_pydantic import BashTask, Dag, PythonTask

dag = Dag(
    dag_id="daily-etl",
    schedule="0 2 * * *",
    start_date="2025-01-01",
    catchup=False,
    default_args={"owner": "data", "retries": 2},
    tasks={
        "extract": BashTask(bash_command="python /opt/etl/extract.py"),
        "load": PythonTask(
            python_callable="builtins.print",
            op_kwargs={"table": "events"},
            dependencies=["extract"],
        ),
    },
)
```

Task dictionary keys become `task_id` values when the DAG is rendered or
instantiated. `dependencies` contains upstream task IDs.

## How to render deployable Python

Render one DAG model and write the result:

```python
from pathlib import Path

source = dag.render()
Path("generated/daily_etl.py").parent.mkdir(parents=True, exist_ok=True)
Path("generated/daily_etl.py").write_text(source)
```

To render a complete `airflow-config` collection, use
`Configuration.generate()` instead. It avoids rewriting unchanged files.

## How to instantiate models in memory

Instantiate a model inside an Airflow DAG context:

```python
from airflow import DAG

with DAG(dag_id="daily-etl", schedule=None) as airflow_dag:
    dag.instantiate(dag=airflow_dag)
```

For declarative configurations, `Configuration.generate_in_mem()` registers
every configured DAG in the calling module.

## How to use sensors and branching

Mix sensors and operators in one task mapping:

```python
from airflow_pydantic import BranchPythonTask, FileSensor

dag.tasks.update(
    {
        "wait-for-input": FileSensor(
            filepath="/data/incoming/events.json",
            poke_interval=30,
            timeout=3600,
        ),
        "choose-path": BranchPythonTask(
            python_callable="builtins.print",
            dependencies=["wait-for-input"],
        ),
    }
)
```

Refer to the [API reference](api.html.md) for every operator and sensor model.

## How to use custom timetables

Pass a timetable model as the DAG schedule:

```python
from datetime import timedelta

from airflow_pydantic import CronTriggerTimetable, MultipleCronTriggerTimetable

dag.schedule = CronTriggerTimetable(
    cron="0 9 * * mon-fri",
    timezone="America/New_York",
    interval=timedelta(hours=1),
)

multiple = MultipleCronTriggerTimetable(
    crons=["0 9 * * mon", "0 9 * * thu"],
    timezone="America/New_York",
)
```

Available timetable models also include data-interval, delta, and event
timetables.

## How to configure pools and variables

Use a pool model anywhere a task accepts `pool`:

```python
from airflow_pydantic import BashTask, Pool, Variable

task = BashTask(
    bash_command="python /opt/jobs/large.py",
    pool=Pool(pool="warehouse", slots=8, description="Warehouse capacity"),
    pool_slots=2,
)

variable = Variable(key="report_settings", deserialize_json=True)
```

Models defer Airflow database access until runtime. A `Variable` can therefore
remain serializable inside configuration.

## How to define typed DAG parameters

Attach Airflow `Param` definitions to the DAG model:

```python
from airflow_pydantic import Dag, Param

parameterized = Dag(
    dag_id="parameterized-report",
    params={
        "limit": Param(
            100,
            type="integer",
            minimum=1,
            title="Row limit",
        )
    },
)
```

Tasks can access the value through Airflow templates such as
`{{ params.limit }}`.

## How to run remote and downstream workflows

Use the SSH and trigger models for provider-backed operations:

```python
from airflow_pydantic import SSHTask, TriggerDagRunTask

remote = SSHTask(
    ssh_conn_id="reports-host",
    command="python /opt/jobs/report.py",
)

trigger = TriggerDagRunTask(
    trigger_dag_id="downstream-report",
    wait_for_completion=True,
    dependencies=["remote"],
)
```

Install the matching Airflow providers in the scheduler and worker environment.

## How to reuse task settings in Python

Pass an argument model through `template`:

```python
from airflow_pydantic import BashTask, BashTaskArgs

shell_defaults = BashTaskArgs(
    retries=2,
    env={"MODE": "production"},
)

report = BashTask(
    template=shell_defaults,
    bash_command="python /opt/jobs/report.py",
)
```

Fields on the concrete task override fields inherited from the template.

## How to compose shell environments

Build activation and working-directory commands with `BashCommands`:

```python
from airflow_pydantic import BashCommands

command = BashCommands(
    commands=["source /opt/venv/bin/activate", "python report.py"],
    cwd="/opt/reports",
    login=True,
)

print(str(command))
```

Use `in_virtualenv()`, `in_conda()`, `in_bash()`, and `link()` for the matching
single-purpose command transformations.

## How to serialize and validate models

Use normal Pydantic methods for storage and round trips:

```python
payload = dag.model_dump(mode="json")
restored = Dag.model_validate(payload)

json_payload = dag.model_dump_json(indent=2)
restored_from_json = Dag.model_validate_json(json_payload)
```

Serialization retains the import paths needed to reconstruct operator,
callable, timetable, and connection types.

## How to use models with airflow-config

Select exported model classes with Hydra `_target_` entries:

```yaml
dags:
  remote-report:
    schedule: "@daily"
    tasks:
      report:
        _target_: airflow_pydantic.SSHTask
        ssh_conn_id: reports-host
        command: python /opt/jobs/report.py
```

See the [airflow-config tutorial](https://github.com/airflow-laminar/airflow-config/blob/main/docs/src/tutorial.md)
for loading and materialization.

## How to integrate long-running and imported schedules

Use task models from the runtime integration that owns the job:

- [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) for supervisord processes modeled by [supervisor-pydantic](https://github.com/airflow-laminar/supervisor-pydantic).
- [airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) for services modeled by [systemd-pydantic](https://github.com/airflow-laminar/systemd-pydantic).
- [airflow-cron](https://github.com/airflow-laminar/airflow-cron) for schedules modeled by [cron-pydantic](https://github.com/airflow-laminar/cron-pydantic).
