from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from .instantiate import DagInstantiateMixin
from .render import DagRenderMixin
from .task import Task, TaskArgs
from .utils import DatetimeArg, ScheduleArg

__all__ = (
    "DagArgs",
    "Dag",
)


class DagArgs(BaseModel):
    # DAG args
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG

    # NOTE: All fields here should be default None
    description: Optional[str] = Field(default=None, description="The description for the DAG to e.g. be shown on the webserver")
    schedule: Optional[ScheduleArg] = Field(
        default=None,
        description="Defines the rules according to which DAG runs are scheduled. Can accept cron string, timedelta object, Timetable, or list of Dataset objects. If this is not provided, the DAG will be set to the default schedule timedelta(days=1). See also Customizing DAG Scheduling with Timetables.",
        union_mode="left_to_right",
    )
    start_date: Optional[DatetimeArg] = Field(default=None, description="The timestamp from which the scheduler will attempt to backfill")
    end_date: Optional[DatetimeArg] = Field(
        default=None, description="A date beyond which your DAG won’t run, leave to None for open-ended scheduling"
    )
    # template_searchpath: Optional[List[str]] = Field(default_factory=None, description="This list of folders (non-relative) defines where jinja will look for your templates. Order matters. Note that jinja/airflow includes the path of your DAG file by default")
    # template_undefined (type[jinja2.StrictUndefined]) – Template undefined type.
    # user_defined_macros (dict | None) – a dictionary of macros that will be exposed in your jinja templates. For example, passing dict(foo='bar') to this argument allows you to {{ foo }} in all jinja templates related to this DAG. Note that you can pass any type of object here.
    # user_defined_filters (dict | None) – a dictionary of filters that will be exposed in your jinja templates. For example, passing dict(hello=lambda name: 'Hello %s' % name) to this argument allows you to {{ 'world' | hello }} in all jinja templates related to this DAG.
    # default_args (dict | None) – A dictionary of default parameters to be used as constructor keyword parameters when initialising operators. Note that operators have the same hook, and precede those defined here, meaning that if your dict contains ‘depends_on_past’: True here and ‘depends_on_past’: False in the operator’s call default_args, the actual value will be False.
    # params (collections.abc.MutableMapping | None) – a dictionary of DAG level parameters that are made accessible in templates, namespaced under params. These params can be overridden at the task level.
    max_active_tasks: Optional[int] = Field(default=None, description="the number of task instances allowed to run concurrently", gt=0)
    max_active_runs: Optional[int] = Field(
        default=None,
        description="maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won’t create new active DAG runs",
        gt=0,
    )
    # max_consecutive_failed_dag_runs: Optional[int] = Field(default=None, description="maximum number of consecutive failed DAG runs, beyond this the scheduler will disable the DAG")
    # dagrun_timeout (datetime.timedelta | None) – specify how long a DagRun should be up before timing out / failing, so that new DagRuns can be created.
    # sla_miss_callback (None | SLAMissCallback | list[SLAMissCallback]) – specify a function or list of functions to call when reporting SLA timeouts. See sla_miss_callback for more information about the function signature and parameters that are passed to the callback.
    default_view: Optional[Literal["grid", "graph", "duration", "gantt", "landing_times"]] = Field(
        default=None,
        description="Specify DAG default view (grid, graph, duration, gantt, landing_times), default grid",
    )
    orientation: Optional[Literal["LR", "TB", "RL", "BT"]] = Field(
        default=None, description="Specify DAG orientation in graph view (LR, TB, RL, BT), default LR"
    )
    catchup: Optional[bool] = Field(default=None, description="Perform scheduler catchup (or only run latest)? Defaults to False")
    # on_failure_callback (None | DagStateChangeCallback | list[DagStateChangeCallback]) – A function or list of functions to be called when a DagRun of this dag fails. A context dictionary is passed as a single parameter to this function.
    # on_success_callback (None | DagStateChangeCallback | list[DagStateChangeCallback]) – Much like the on_failure_callback except that it is executed when the dag succeeds.
    # access_control (dict | None) – Specify optional DAG-level actions, e.g., “{‘role1’: {‘can_read’}, ‘role2’: {‘can_read’, ‘can_edit’, ‘can_delete’}}”
    is_paused_upon_creation: Optional[bool] = Field(
        default=None,
        description="Specifies if the dag is paused when created for the first time. If the dag exists already, this flag will be ignored.",
    )
    # jinja_environment_kwargs (dict | None) – additional configuration options to be passed to Jinja Environment for template rendering
    # render_template_as_native_obj (bool) – If True, uses a Jinja NativeEnvironment to render templates as native Python types. If False, a Jinja Environment is used to render templates as string values.
    tags: Optional[List[str]] = Field(default=None, description="List of tags to help filtering DAGs in the UI.")
    # owner_links (dict[str, str] | None) – Dict of owners and their links, that will be clickable on the DAGs view UI. Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link. e.g: {“dag_owner”: “https://airflow.apache.org/”}
    # auto_register (bool) – Automatically register this DAG when it is used in a with block
    # fail_stop (bool) – Fails currently running tasks when task in DAG fails. Warning: A fail stop dag can only have tasks with the default trigger rule (“all_success”). An exception will be thrown if any task in a fail stop dag has a non default trigger rule.
    dag_display_name: Optional[str] = Field(default=None, description="The display name of the DAG which appears on the UI.")

    # Extras
    enabled: Optional[bool] = Field(default=None, description="Whether the DAG is enabled")


class Dag(DagArgs, DagRenderMixin, DagInstantiateMixin):
    dag_id: Optional[str] = Field(
        default=None, description="The id of the DAG; must consist exclusively of alphanumeric characters, dashes, dots and underscores (all ASCII)"
    )
    default_args: Optional[TaskArgs] = Field(default=None, description="Default arguments for tasks in the DAG")
    tasks: Optional[Dict[str, Task]] = Field(default_factory=dict, description="List of tasks in the DAG")

    # TODO: Validate all task dependencies exist
