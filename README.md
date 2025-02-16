# airflow-pydantic

Pydantic models for Apache Airflow

[![Build Status](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-pydantic/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-pydantic/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-pydantic)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-pydantic)](https://github.com/airflow-laminar/airflow-pydantic)
[![PyPI](https://img.shields.io/pypi/v/airflow-pydantic.svg)](https://pypi.python.org/pypi/airflow-pydantic)

## Overview

[Pydantic](https://docs.pydantic.dev/latest/) models of Apache Airflow data structures:

- [Dag Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/dag/index.html#airflow.models.dag.DAG)
- [Task Arguments](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator)
- [PythonOperatorArgs](https://airflow.apache.org/docs/apache-airflow-providers-standard/2.10.4/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator)
- [BashOperatorArgs](https://airflow.apache.org/docs/apache-airflow-providers-standard/2.10.4/_api/airflow/providers/standard/operators/bash/index.html)
- [SSHOperatorArgs](https://airflow.apache.org/docs/apache-airflow-providers-ssh/2.10.4/_api/airflow/providers/ssh/operators/ssh/index.html)


> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
