# Getting Started

## Why airflow-pydantic?

`airflow-pydantic` enables **declarative, configuration-driven DAG definitions** for Apache Airflow. Instead of writing DAGs in Python, you can define them in YAML, JSON, or other configuration formats using frameworks like [airflow-config](https://github.com/airflow-laminar/airflow-config).

**Key Benefits:**

- **Declarative DAGs**: Define workflows in configuration files, not code
- **Separation of Concerns**: Keep business logic separate from DAG structure
- **Environment Management**: Easy per-environment configuration overrides
- **Validation**: Pydantic models provide automatic validation and type checking
- **Serialization**: Full JSON/YAML serialization support for all Airflow constructs
- **Code Generation**: Generate Python DAG files from configuration when needed

## Installation

Install airflow-pydantic from PyPI:

```bash
pip install airflow-pydantic
```

For use with Apache Airflow 2.x:

```bash
pip install airflow-pydantic[airflow]
```

For use with Apache Airflow 3.x:

```bash
pip install airflow-pydantic[airflow3]
```

For the full laminar stack:

```bash
pip install airflow-pydantic[laminar]
```

## Basic Usage

### Declarative DAGs with airflow-config (Recommended)

The recommended way to use `airflow-pydantic` is with [airflow-config](https://github.com/airflow-laminar/airflow-config) for fully declarative DAG definitions:

```yaml
# config/etl_dag.yaml
default_args:
  _target_: airflow_pydantic.TaskArgs
  owner: data-team
  retries: 3
  retry_delay: 300  # 5 minutes

default_dag_args:
  _target_: airflow_pydantic.DagArgs
  schedule: "0 6 * * *"
  start_date: "2024-01-01"
  catchup: false
  tags:
    - etl
    - production
```

```python
# dags/etl_dag.py
from airflow_config import DAG, load_config

config = load_config("config", "etl_dag")
dag = DAG(dag_id="etl-pipeline", config=config)
```

This approach allows you to manage DAG configurations separately from code, making it easy to:
- Override settings per environment (dev, staging, production)
- Version control your DAG configurations
- Validate configurations before deployment
- Generate documentation from configuration

### Creating a DAG Programmatically

For programmatic use cases, the core of `airflow-pydantic` is the `Dag` model, which provides a Pydantic-validated representation of an Apache Airflow DAG:

```python
from datetime import datetime
from airflow_pydantic import Dag, DagArgs, TaskArgs

# Create DAG arguments
dag_args = DagArgs(
    description="My first Pydantic DAG",
    schedule="0 0 * * *",  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "pydantic"],
)

# Create a DAG model
dag = Dag(
    dag_id="my-pydantic-dag",
    **dag_args.model_dump(exclude_unset=True),
    tasks={},
)
```

### Adding Tasks

Add tasks to your DAG using Pydantic task models:

```python
from airflow_pydantic import (
    Dag,
    PythonTask,
    PythonTaskArgs,
    BashTask,
    BashTaskArgs,
)


def my_python_callable(**kwargs):
    print("Hello from Python!")


# Create task arguments
python_args = PythonTaskArgs(
    python_callable=my_python_callable,
    op_kwargs={"message": "Hello"},
)

bash_args = BashTaskArgs(
    bash_command="echo 'Hello from Bash!'",
)

# Create a DAG with tasks
dag = Dag(
    dag_id="my-task-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={
        "python_task": PythonTask(
            task_id="python_task",
            **python_args.model_dump(exclude_unset=True),
        ),
        "bash_task": BashTask(
            task_id="bash_task",
            **bash_args.model_dump(exclude_unset=True),
        ),
    },
)
```

### Instantiating a DAG

Convert your Pydantic DAG model into a real Airflow DAG using the `instantiate()` method:

```python
from airflow_pydantic import Dag

dag_model = Dag(
    dag_id="example-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={},
)

# Create an Airflow DAG instance
airflow_dag = dag_model.instantiate()

# Or use as a context manager
with dag_model.instantiate() as dag:
    # Add additional tasks or configure the DAG
    pass
```

### Rendering a DAG

Generate Python code for your DAG using the `render()` method. This is useful for generating standalone DAG files:

```python
dag = Dag(
    dag_id="rendered-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={},
)

# Get Python code representation
python_code = dag.render()
print(python_code)
```

## Working with Pools and Variables

### Pools

Define Airflow pools using the `Pool` model:

```python
from airflow_pydantic import Pool, BashTaskArgs

pool = Pool(
    pool="my-pool",
    slots=5,
    description="A custom pool for my tasks",
)

task_args = BashTaskArgs(
    bash_command="echo 'Using a pool'",
    pool=pool,
)
```

### Variables

Reference Airflow variables using the `Variable` model:

```python
from airflow_pydantic import Variable

var = Variable(
    key="my_variable",
    description="A configuration variable",
    deserialize_json=True,
)

# Get the variable value at runtime
value = var.get()
```

## Serialization and Validation

Since all models are Pydantic-based, you get automatic serialization and validation:

```python
from airflow_pydantic import Dag

dag = Dag(
    dag_id="serializable-dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tasks={},
)

# Serialize to dict
dag_dict = dag.model_dump(exclude_unset=True)

# Serialize to JSON
dag_json = dag.model_dump_json(exclude_unset=True)

# Validate from dict
validated_dag = Dag.model_validate(dag_dict)

# Validate from JSON
validated_dag = Dag.model_validate_json(dag_json)
```

## Next Steps

- See the [Examples](examples.md) for more detailed usage patterns, including airflow-config integration
- Check the [API Reference](API.md) for complete documentation of all models
- Explore [airflow-config](https://github.com/airflow-laminar/airflow-config) for the full declarative DAG experience
