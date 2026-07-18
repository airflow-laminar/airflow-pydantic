# Tutorial: build a declarative DAG

In this tutorial, we will describe an extract-and-report workflow in YAML,
load it through `airflow-config`, and inspect the resulting DAG models.

## Install the packages

For Airflow 3, run:

```bash
pip install 'airflow-pydantic[airflow3]' airflow-config
```

## Describe the workflow

Create `config/report.yaml` beside your DAG file:

```yaml
# @package _global_
_target_: airflow_config.Configuration

default_task_args:
  _target_: airflow_config.TaskArgs
  owner: analytics
  retries: 2

dags:
  daily-report:
    schedule: "0 6 * * *"
    start_date: "2025-01-01"
    catchup: false
    tags: [analytics, tutorial]
    tasks:
      extract:
        _target_: airflow_config.BashTask
        bash_command: python /opt/jobs/extract.py
      report:
        _target_: airflow_config.BashTask
        bash_command: python /opt/jobs/report.py
        dependencies: [extract]
```

## Load the models

Create `daily_report.py` beside the `config` directory:

```python
from airflow_config import load_config

config = load_config("config", "report")

dag_model = config.dags["daily-report"]
print(dag_model.schedule)
print(list(dag_model.tasks))
print(dag_model.tasks["report"].dependencies)
```

Run the file with Python. The output should be:

```text
0 6 * * *
['extract', 'report']
['extract']
```

Notice that dictionary keys supplied the DAG and task IDs, while each `_target_`
selected a concrete `airflow-pydantic` model.

## Register the DAG

Add one line to the end of `daily_report.py`:

```python
config.generate_in_mem()
```

Place the Python and YAML files in an Airflow DAG folder, preserving their
relative paths. Then list the DAG and tasks:

```bash
airflow dags list | grep daily-report
airflow tasks list daily-report
```

The task list contains `extract` and `report`. Airflow displays `report` after
`extract` in the graph because the model resolved the dependency.

You have now used validated Pydantic models to build and register a declarative
Airflow DAG.
