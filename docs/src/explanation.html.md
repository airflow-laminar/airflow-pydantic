# Why model Airflow configuration

Airflow DAG files are executable Python, but most of their content is data:
schedules, retry policies, operator arguments, dependencies, pools, and
connection references. `airflow-pydantic` makes that data explicit before
Airflow imports it.

## Validation before DAG parsing

Airflow normally discovers configuration mistakes while importing a DAG file.
Pydantic models move many failures earlier: invalid field types, unsupported
values, missing operator arguments, and malformed import paths can fail during
configuration loading or CI. The resulting models are also straightforward to
compare, serialize, template, and test without running a scheduler.

This boundary is intentionally incomplete. Airflow remains responsible for
operator execution, templates, scheduling, connections, and metadata. The
models describe those objects; they do not reimplement Airflow.

## One model, two materialization paths

A `Dag` can render deterministic Python source or instantiate native Airflow
objects. Rendering produces reviewable deployment artifacts and removes the
runtime configuration loader from the scheduler path. In-memory instantiation
avoids generated files and responds directly to configuration changes.

Both paths use the same validated task graph. Choosing between them is an
operational decision, not a modeling decision. `airflow-config` exposes both
through `generate()` and `generate_in_mem()`.

## Why airflow-config is separate

`airflow-pydantic` is the typed vocabulary: DAGs, tasks, sensors, schedules,
pools, variables, hosts, and ports. `airflow-config` supplies composition:
Hydra search paths, defaults, overrides, templates, extensions, and loading.

Keeping the packages separate lets Python users construct models without Hydra
and lets configuration users combine models from other packages through
`_target_`. The separation also keeps runtime integrations independent of the
core DAG schema.

## Airflow version boundaries

Airflow 2 and Airflow 3 moved or renamed several operator, sensor, and timetable
types. The public model names remain stable where the underlying behavior is
equivalent, while import resolution occurs when a model is instantiated or
rendered. Version-specific installation extras provide the matching Airflow
dependency set.

The abstraction cannot erase semantic differences. Fields removed by Airflow
may be ignored or rejected, and provider-specific models still require their
providers. CI should validate generated DAGs against every supported Airflow
version used in deployment.

## The wider ecosystem

The same task-model protocol lets integrations participate in an ordinary DAG:

- [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) delegates long-running processes to [supervisor-pydantic](https://github.com/airflow-laminar/supervisor-pydantic).
- [airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) delegates services to [systemd-pydantic](https://github.com/airflow-laminar/systemd-pydantic).
- [airflow-cron](https://github.com/airflow-laminar/airflow-cron) translates [cron-pydantic](https://github.com/airflow-laminar/cron-pydantic) jobs into DAG models.
- [airflow-balancer](https://github.com/airflow-laminar/airflow-balancer) adds host and port selection.
- [airflow-common](https://github.com/airflow-laminar/airflow-common) adds reusable orchestration models.

These integrations extend the vocabulary while `airflow-config` remains the
composition layer.
