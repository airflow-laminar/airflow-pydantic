# Examples

This page provides comprehensive examples of using `airflow-pydantic` for various use cases.

## Declarative DAGs with airflow-config (Recommended)

The primary use of `airflow-pydantic` is building **declarative, configuration-driven DAGs**. Using [airflow-config](https://github.com/airflow-laminar/airflow-config), you can define your entire DAG structure in YAML:

### Basic YAML DAG Definition

```yaml
# config/etl/pipeline.yaml
# @package _global_
_target_: airflow_config.Configuration

default_args:
  _target_: airflow_pydantic.TaskArgs
  owner: data-team
  retries: 3
  retry_delay: 300
  email_on_failure: true
  email:
    - alerts@example.com

default_dag_args:
  _target_: airflow_pydantic.DagArgs
  description: "ETL Pipeline for data processing"
  schedule: "0 6 * * *"
  start_date: "2024-01-01"
  catchup: false
  tags:
    - etl
    - production
```

```python
# dags/etl_pipeline.py
from airflow_config import DAG, load_config

config = load_config("config/etl", "pipeline")
dag = DAG(dag_id="etl-pipeline", config=config)
```

### Environment-Specific Configurations

With airflow-config, you can easily manage different configurations per environment:

```yaml
# config/base.yaml - Shared defaults
default_args:
  _target_: airflow_pydantic.TaskArgs
  owner: data-team
  retries: 3

# config/production.yaml - Production overrides
defaults:
  - base

default_args:
  retries: 5
  email_on_failure: true

default_dag_args:
  _target_: airflow_pydantic.DagArgs
  schedule: "0 */4 * * *"  # Every 4 hours in production

# config/development.yaml - Development overrides
defaults:
  - base

default_args:
  retries: 1
  email_on_failure: false

default_dag_args:
  _target_: airflow_pydantic.DagArgs
  schedule: "@daily"  # Less frequent in dev
  catchup: false
```

### Defining Tasks in YAML

```yaml
# config/my_dag.yaml
_target_: airflow_config.Configuration

tasks:
  extract:
    _target_: airflow_pydantic.BashTask
    task_id: extract
    bash_command: "python extract.py"

  transform:
    _target_: airflow_pydantic.PythonTask
    task_id: transform
    python_callable: "my_module.transform_data"
    dependencies:
      - extract

  load:
    _target_: airflow_pydantic.BashTask
    task_id: load
    bash_command: "python load.py"
    dependencies:
      - transform
```

### Benefits of Declarative DAGs

| Approach | Traditional Python | Declarative YAML |
|----------|-------------------|------------------|
| Configuration | Mixed with code | Separate files |
| Environment overrides | Conditional logic | File inheritance |
| Validation | Runtime only | Build-time with Pydantic |
| Version control | Code changes | Config changes |
| Non-developer access | Requires Python | Edit YAML |

## Programmatic DAG Examples

For cases where programmatic DAG creation is preferred:

## DAG Examples

### Simple DAG with Python and Bash Tasks

```python
from datetime import datetime, timedelta
from airflow_pydantic import (
    Dag,
    DagArgs,
    TaskArgs,
    PythonTask,
    PythonTaskArgs,
    BashTask,
    BashTaskArgs,
)


def process_data(**kwargs):
    print("Processing data...")
    return {"status": "success"}


dag_args = DagArgs(
    description="A simple ETL DAG",
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "example"],
)

task_args = TaskArgs(
    owner="data-team",
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_failure=True,
    email=["alerts@example.com"],
)

dag = Dag(
    dag_id="simple-etl-dag",
    **dag_args.model_dump(exclude_unset=True),
    default_args=task_args,
    tasks={
        "extract": BashTask(
            task_id="extract",
            bash_command="echo 'Extracting data...'",
        ),
        "transform": PythonTask(
            task_id="transform",
            python_callable=process_data,
            dependencies=["extract"],
        ),
        "load": BashTask(
            task_id="load",
            bash_command="echo 'Loading data...'",
            dependencies=["transform"],
        ),
    },
)
```

### DAG with Sensors

```python
from datetime import datetime, time, timedelta
from airflow_pydantic import (
    Dag,
    PythonTask,
    TimeSensor,
    TimeSensorArgs,
    WaitSensor,
    WaitSensorArgs,
    DateTimeSensor,
    DateTimeSensorArgs,
)


def run_after_wait(**kwargs):
    print("Running after wait!")


dag = Dag(
    dag_id="sensor-example-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={
        "wait_for_time": TimeSensor(
            task_id="wait_for_time",
            target_time=time(9, 0),  # Wait until 9 AM
        ),
        "wait_for_duration": WaitSensor(
            task_id="wait_for_duration",
            time_to_wait=timedelta(minutes=10),
            dependencies=["wait_for_time"],
        ),
        "run_task": PythonTask(
            task_id="run_task",
            python_callable=run_after_wait,
            dependencies=["wait_for_duration"],
        ),
    },
)
```

### DAG with Custom Schedule (Timetables)

```python
from datetime import datetime
from airflow_pydantic import Dag, BashTask
from airflow_pydantic.utils import CronTriggerTimetable, MultipleCronTriggerTimetable

# Single cron with specific timezone
dag_single = Dag(
    dag_id="cron-trigger-dag",
    schedule=CronTriggerTimetable(
        cron="0 9 * * MON-FRI",  # 9 AM on weekdays
        timezone="America/New_York",
    ),
    start_date=datetime(2024, 1, 1),
    tasks={
        "weekday_task": BashTask(
            task_id="weekday_task",
            bash_command="echo 'Running on weekdays!'",
        ),
    },
)

# Multiple cron expressions
dag_multi = Dag(
    dag_id="multi-cron-dag",
    schedule=MultipleCronTriggerTimetable(
        cron_defs=["0 9 * * MON-FRI", "0 12 * * SAT,SUN"],
        timezone="UTC",
    ),
    start_date=datetime(2024, 1, 1),
    tasks={
        "flexible_task": BashTask(
            task_id="flexible_task",
            bash_command="echo 'Running!'",
        ),
    },
)
```

## Task Examples

### Python Task with Arguments

```python
from airflow_pydantic import PythonTask, PythonTaskArgs


def greet(name, greeting="Hello", **kwargs):
    message = f"{greeting}, {name}!"
    print(message)
    return message


python_args = PythonTaskArgs(
    python_callable=greet,
    op_args=["World"],
    op_kwargs={"greeting": "Hi"},
    show_return_value_in_logs=True,
)

task = PythonTask(
    task_id="greet-task",
    **python_args.model_dump(exclude_unset=True),
)
```

### Bash Task with Environment Variables

```python
from airflow_pydantic import BashTask, BashTaskArgs

bash_args = BashTaskArgs(
    bash_command="echo $MY_VAR && python script.py",
    env={"MY_VAR": "hello", "DEBUG": "true"},
    append_env=True,
    cwd="/opt/scripts",
    output_encoding="utf-8",
)

task = BashTask(
    task_id="bash-with-env",
    **bash_args.model_dump(exclude_unset=True),
)
```

### SSH Task

```python
from airflow_pydantic import SSHTask, SSHTaskArgs
from airflow_pydantic.utils import BashCommands

ssh_args = SSHTaskArgs(
    ssh_conn_id="my_ssh_connection",
    command=BashCommands(
        commands=[
            "cd /opt/app",
            "git pull",
            "pip install -r requirements.txt",
            "python run.py",
        ],
        login=True,
    ),
    environment={"PYTHONPATH": "/opt/app"},
    cmd_timeout=300,
)

task = SSHTask(
    task_id="remote-deploy",
    **ssh_args.model_dump(exclude_unset=True),
)
```

### Trigger DAG Run Task

```python
from airflow_pydantic import TriggerDagRunTask, TriggerDagRunTaskArgs

trigger_args = TriggerDagRunTaskArgs(
    trigger_dag_id="downstream-dag",
    wait_for_completion=True,
    poke_interval=30,
    conf={"triggered_by": "upstream-dag"},
)

task = TriggerDagRunTask(
    task_id="trigger-downstream",
    **trigger_args.model_dump(exclude_unset=True),
)
```

## Utility Examples

### Using Pools

```python
from airflow_pydantic import Pool, BashTask, BashTaskArgs

# Define a pool
pool = Pool(
    pool="resource-limited-pool",
    slots=3,
    description="Pool for resource-intensive tasks",
)

# Use pool in task
task = BashTask(
    task_id="pooled-task",
    bash_command="heavy_computation.sh",
    pool=pool,
    pool_slots=1,
)
```

### Using Variables

```python
from airflow_pydantic import Variable

# Define variables
api_key_var = Variable(
    key="API_KEY",
    description="API key for external service",
    is_encrypted=True,
)

config_var = Variable(
    key="APP_CONFIG",
    description="Application configuration",
    deserialize_json=True,
)

# Get values at runtime
api_key = api_key_var.get()
config = config_var.get()  # Returns parsed JSON
```

### Using BashCommands Helper

```python
from airflow_pydantic.utils import BashCommands, in_bash, in_conda, in_virtualenv

# Using BashCommands model
commands = BashCommands(
    commands=[
        "echo 'Starting...'",
        "python process.py",
        "echo 'Done!'",
    ],
    login=True,
    cwd="/opt/app",
    env={"DEBUG": "1"},
)

# Convert to bash command string
bash_string = commands.model_dump()

# Using helper functions
conda_cmd = in_conda("myenv", "python script.py")
# Result: "micromamba activate myenv && python script.py"

venv_cmd = in_virtualenv("/opt/venv", "python script.py")
# Result: "source /opt/venv/bin/activate && python script.py"

bash_cmd = in_bash("echo 'hello'", login=True, cwd="/tmp")
# Result: bash -lc 'cd /tmp; echo 'hello''
```

### Using DAG Parameters

```python
from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from airflow_pydantic import Dag, BashTask


class MyDagParams(BaseModel):
    environment: str = "production"
    debug: bool = False
    batch_size: Optional[int] = None


dag = Dag(
    dag_id="parameterized-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    params=MyDagParams,
    tasks={
        "run_with_params": BashTask(
            task_id="run_with_params",
            bash_command='echo "Running in {{ params.environment }}"',
        ),
    },
)
```

## Serialization Examples

### Save and Load DAG Configuration

```python
import json
from datetime import datetime
from airflow_pydantic import Dag, BashTask

# Create a DAG
dag = Dag(
    dag_id="serializable-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    description="A DAG that can be serialized",
    tasks={
        "task1": BashTask(task_id="task1", bash_command="echo 'hello'"),
    },
)

# Serialize to JSON
dag_json = dag.model_dump_json(exclude_unset=True, indent=2)
print(dag_json)

# Save to file
with open("dag_config.json", "w") as f:
    f.write(dag_json)

# Load from file
with open("dag_config.json", "r") as f:
    loaded_dag = Dag.model_validate_json(f.read())
```

### Generate DAG Python Code

```python
from datetime import datetime
from airflow_pydantic import Dag, BashTask, PythonTask


def my_func(**kwargs):
    pass


dag = Dag(
    dag_id="generated-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={
        "bash_task": BashTask(
            task_id="bash_task",
            bash_command="echo 'Hello'",
        ),
        "python_task": PythonTask(
            task_id="python_task",
            python_callable="my_module.my_func",
            dependencies=["bash_task"],
        ),
    },
)

# Render to Python code
python_code = dag.render()
print(python_code)

# Save to a .py file for use in Airflow
with open("dags/generated_dag.py", "w") as f:
    f.write(python_code)
```

## Code Generation with render()

The `render()` method is a powerful feature that generates valid Python code from Pydantic models. This enables code generation workflows, debugging, and migration paths.

### Rendering Individual Tasks

You can render individual operators and sensors:

```python
from airflow_pydantic import BashTask, PythonTask, SSHTask
from datetime import timedelta

# Render a BashTask
bash_task = BashTask(
    task_id="extract_data",
    bash_command="python extract.py --date {{ ds }}",
    retries=3,
    retry_delay=timedelta(minutes=5),
)
print(bash_task.render())
# Output:
# BashOperator(
#     task_id="extract_data",
#     bash_command="python extract.py --date {{ ds }}",
#     retries=3,
#     retry_delay=timedelta(minutes=5),
# )

# Render an SSHTask
ssh_task = SSHTask(
    task_id="remote_job",
    ssh_conn_id="my_server",
    command="./run_job.sh",
    cmd_timeout=300,
)
print(ssh_task.render())
```

### Rendering Complete DAGs

Render an entire DAG with all its tasks and dependencies:

```python
from datetime import datetime, timedelta
from airflow_pydantic import Dag, DagArgs, TaskArgs, BashTask, PythonTask

dag = Dag(
    dag_id="etl-pipeline",
    description="Daily ETL pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=TaskArgs(
        owner="data-team",
        retries=3,
        retry_delay=timedelta(minutes=5),
    ),
    tasks={
        "extract": BashTask(
            task_id="extract",
            bash_command="python extract.py",
        ),
        "transform": PythonTask(
            task_id="transform",
            python_callable="etl.transform.run",
            dependencies=["extract"],
        ),
        "load": BashTask(
            task_id="load",
            bash_command="python load.py",
            dependencies=["transform"],
        ),
    },
)

# Generate the complete DAG file
dag_code = dag.render()
print(dag_code)
```

### CI/CD Code Generation

Use `render()` in CI/CD pipelines to generate DAG files from configuration:

```python
# scripts/generate_dags.py
import json
from pathlib import Path
from airflow_pydantic import Dag

def generate_dags(config_dir: Path, output_dir: Path):
    """Generate DAG files from JSON configuration."""
    for config_file in config_dir.glob("*.json"):
        # Load configuration
        with open(config_file) as f:
            config = json.load(f)

        # Create and validate DAG model
        dag = Dag.model_validate(config)

        # Render to Python code
        python_code = dag.render()

        # Write DAG file
        output_file = output_dir / f"{dag.dag_id}.py"
        output_file.write_text(python_code)
        print(f"Generated: {output_file}")

if __name__ == "__main__":
    generate_dags(
        config_dir=Path("config/dags"),
        output_dir=Path("dags"),
    )
```

### Rendering Timetables

Custom timetables can also be rendered:

```python
from airflow_pydantic import Dag, BashTask
from airflow_pydantic.utils import CronTriggerTimetable, MultipleCronTriggerTimetable

dag = Dag(
    dag_id="multi-schedule-dag",
    schedule=MultipleCronTriggerTimetable(
        cron_defs=["0 9 * * MON-FRI", "0 12 * * SAT,SUN"],
        timezone="America/New_York",
    ),
    start_date=datetime(2024, 1, 1),
    tasks={
        "task": BashTask(
            task_id="task",
            bash_command="echo 'Running!'",
        ),
    },
)

print(dag.render())
```

## Additional airflow-config Patterns

For more advanced airflow-config usage patterns, see the [declarative DAG examples](#declarative-dags-with-airflow-config-recommended) at the top of this page.

### Using Pools in YAML

```yaml
# config/pooled_dag.yaml
pools:
  resource_pool:
    _target_: airflow_pydantic.Pool
    pool: resource-limited-pool
    slots: 5
    description: "Pool for resource-intensive tasks"

tasks:
  heavy_task:
    _target_: airflow_pydantic.BashTask
    task_id: heavy_task
    bash_command: "python heavy_computation.py"
    pool: resource-limited-pool
    pool_slots: 1
```

### Using Variables in YAML

```yaml
# config/var_dag.yaml
variables:
  api_config:
    _target_: airflow_pydantic.Variable
    key: API_CONFIG
    description: "API configuration"
    deserialize_json: true
```

## Template Pattern

Use the template pattern to create reusable task configurations:

```python
from airflow_pydantic import SSHTask, SSHTaskArgs

# Create a template task
template_task = SSHTask(
    task_id="template",
    ssh_conn_id="default_ssh",
    command="echo 'default'",
    retries=3,
    retry_delay=timedelta(minutes=5),
)

# Create new tasks based on template
task1 = SSHTask(
    task_id="task1",
    template=template_task,
    command="echo 'task 1'",  # Override specific fields
)

task2 = SSHTask(
    task_id="task2",
    template=template_task,
    command="echo 'task 2'",
)
```
