# airflow-pydantic

Typed, serializable models for Apache Airflow DAGs, tasks, sensors, schedules,
and runtime resources.

[![Build Status](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-pydantic/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-pydantic)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-pydantic)](https://github.com/airflow-laminar/airflow-pydantic)
[![PyPI](https://img.shields.io/pypi/v/airflow-pydantic.svg)](https://pypi.python.org/pypi/airflow-pydantic)

```python
from airflow_pydantic import BashTask, Dag

dag = Dag(
    dag_id="daily-report",
    schedule="0 6 * * *",
    start_date="2025-01-01",
    catchup=False,
    tasks={
        "report": BashTask(bash_command="python /opt/jobs/report.py"),
    },
)

print(dag.render())
```

Models validate configuration before Airflow parses it, support Python and
YAML workflows, render standalone DAG source, and instantiate native Airflow
objects. `airflow-config` is the recommended YAML entry point.

## Documentation

- [Tutorial: build a declarative DAG](docs/src/tutorial.md)
- [How-to guides](docs/src/how-to.md)
- [Why model Airflow configuration](docs/src/explanation.md)
- [API reference](docs/src/api.md)

Published documentation is available at
[airflow-laminar.github.io/airflow-pydantic](https://airflow-laminar.github.io/airflow-pydantic/).

## Ecosystem

- [airflow-config](https://github.com/airflow-laminar/airflow-config) loads Hydra/YAML configuration and materializes these models.
- [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) and [supervisor-pydantic](https://github.com/airflow-laminar/supervisor-pydantic) manage supervisord jobs.
- [airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) and [systemd-pydantic](https://github.com/airflow-laminar/systemd-pydantic) manage systemd services.
- [airflow-cron](https://github.com/airflow-laminar/airflow-cron) and [cron-pydantic](https://github.com/airflow-laminar/cron-pydantic) convert crontabs into DAG models.
- [airflow-balancer](https://github.com/airflow-laminar/airflow-balancer) selects hosts, ports, pools, and queues.
- [airflow-common](https://github.com/airflow-laminar/airflow-common) provides reusable operators and orchestration helpers.

#### NOTE
This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
