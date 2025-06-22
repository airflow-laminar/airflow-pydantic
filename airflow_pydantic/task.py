from datetime import timedelta
from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, Field, field_validator

from .instantiate import TaskInstantiateMixin
from .render import TaskRenderMixin
from .utils import DatetimeArg, ImportPath, TriggerRule

__all__ = (
    "TaskArgs",
    "Task",
    "TaskModel",
)


class TaskArgs(BaseModel, extra="allow", validate_assignment=True):
    # Operator Args
    # https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator

    # NOTE: All fields here should be default None
    owner: Optional[str] = Field(
        default=None,
        description="the owner of the task. Using a meaningful description (e.g. user/person/team/role name) to clarify ownership is recommended.",
    )
    email: Optional[List[str]] = Field(default=None, description="the 'to' email address(es) used in email alerts")
    email_on_failure: Optional[bool] = Field(default=None, description="Indicates whether email alerts should be sent when a task failed")
    email_on_retry: Optional[bool] = Field(default=None, description="Indicates whether email alerts should be sent when a task is retried")
    retries: Optional[int] = Field(default=None, description="the number of retries that should be performed before failing the task")
    retry_delay: Optional[timedelta] = Field(default=None, description="delay between retries")
    retry_exponential_backoff: Optional[bool] = Field(
        default=None,
        description="allow progressively longer waits between retries by using exponential backoff algorithm on retry delay (delay will be converted into seconds)",
    )
    max_retry_delay: Optional[timedelta] = Field(default=None, description="maximum delay interval between retries")
    start_date: Optional[DatetimeArg] = Field(
        default=None,
        description="The start_date for the task, determines the execution_date for the first task instance. The best practice is to have the start_date rounded to your DAG’s schedule_interval. Daily jobs have their start_date some day at 00:00:00, hourly jobs have their start_date at 00:00 of a specific hour. Note that Airflow simply looks at the latest execution_date and adds the schedule_interval to determine the next execution_date. It is also very important to note that different tasks’ dependencies need to line up in time. If task A depends on task B and their start_date are offset in a way that their execution_date don’t line up, A’s dependencies will never be met. If you are looking to delay a task, for example running a daily task at 2AM, look into the TimeSensor and TimeDeltaSensor. We advise against using dynamic start_date and recommend using fixed ones. Read the FAQ entry about start_date for more information.",
    )
    end_date: Optional[DatetimeArg] = Field(default=None, description="if specified, the scheduler won’t go beyond this date")
    depends_on_past: Optional[bool] = Field(
        default=None,
        description="when set to true, task instances will run sequentially and only if the previous instance has succeeded or has been skipped. The task instance for the start_date is allowed to run.",
    )
    # wait_for_past_depends_before_skipping: bool = Field(
    #     default=False,
    #     description="when set to true, if the task instance should be marked as skipped, and depends_on_past is true, the ti will stay on None state waiting the task of the previous run",
    # )
    # wait_for_downstream: bool = Field(
    #     default=False,
    #     description="when set to true, an instance of task X will wait for tasks immediately downstream of the previous instance of task X to finish successfully or be skipped before it runs. This is useful if the different instances of a task X alter the same asset, and this asset is used by tasks downstream of task X. Note that depends_on_past is forced to True wherever wait_for_downstream is used. Also note that only tasks immediately downstream of the previous task instance are waited for; the statuses of any tasks further downstream are ignored.",
    # )
    # dag (airflow.models.dag.DAG | None) – a reference to the dag the task is attached to (if any)
    # priority_weight: int = Field(
    #     default=1,
    #     description="priority weight of this task against other task. This allows the executor to trigger higher priority tasks before others when things get backed up. Set priority_weight as a higher number for more important tasks.",
    # )
    # weight_rule: str = Field(
    #     default="downstream",
    #     description="weighting method used for the effective total priority weight of the task. Options are: { downstream | upstream | absolute } default is downstream When set to downstream the effective weight of the task is the aggregate sum of all downstream descendants. As a result, upstream tasks will have higher weight and will be scheduled more aggressively when using positive weight values. This is useful when you have multiple dag run instances and desire to have all upstream tasks to complete for all runs before each dag can continue processing downstream tasks. When set to upstream the effective weight is the aggregate sum of all upstream ancestors. This is the opposite where downstream tasks have higher weight and will be scheduled more aggressively when using positive weight values. This is useful when you have multiple dag run instances and prefer to have each dag complete before starting upstream tasks of other dags. When set to absolute, the effective weight is the exact priority_weight specified without additional weighting. You may want to do this when you know exactly what priority weight each task should have. Additionally, when set to absolute, there is bonus effect of significantly speeding up the task creation process as for very large DAGs. Options can be set as string or using the constants defined in the static class airflow.utils.WeightRule",
    # )
    queue: Optional[str] = Field(
        default=None,
        description="which queue to target when running this job. Not all executors implement queue management, the CeleryExecutor does support targeting specific queues.",
    )
    pool: Optional[str] = Field(
        default=None,
        description="the slot pool this task should run in, slot pools are a way to limit concurrency for certain tasks",
    )
    pool_slots: Optional[int] = Field(
        default=None, description="the number of pool slots this task should use (>= 1) Values less than 1 are not allowed"
    )
    # sla: Optional[datetime] = Field(
    #     default=None,
    #     description="time by which the job is expected to succeed. Note that this represents the timedelta after the period is closed. For example if you set an SLA of 1 hour, the scheduler would send an email soon after 1:00AM on the 2016-01-02 if the 2016-01-01 instance has not succeeded yet. The scheduler pays special attention for jobs with an SLA and sends alert emails for SLA misses. SLA misses are also recorded in the database for future reference. All tasks that share the same SLA time get bundled in a single email, sent soon after that time. SLA notification are sent once and only once for each task instance.",
    # )
    execution_timeout: Optional[timedelta] = Field(
        default=None,
        description="max time allowed for the execution of this task instance, if it goes beyond it will raise and fail.",
    )
    # on_failure_callback (None | airflow.models.abstractoperator.TaskStateChangeCallback | list[airflow.models.abstractoperator.TaskStateChangeCallback]) – a function or list of functions to be called when a task instance of this task fails. a context dictionary is passed as a single parameter to this function. Context contains references to related objects to the task instance and is documented under the macros section of the API.
    # on_execute_callback (None | airflow.models.abstractoperator.TaskStateChangeCallback | list[airflow.models.abstractoperator.TaskStateChangeCallback]) – much like the on_failure_callback except that it is executed right before the task is executed.
    # on_retry_callback (None | airflow.models.abstractoperator.TaskStateChangeCallback | list[airflow.models.abstractoperator.TaskStateChangeCallback]) – much like the on_failure_callback except that it is executed when retries occur.
    # on_success_callback (None | airflow.models.abstractoperator.TaskStateChangeCallback | list[airflow.models.abstractoperator.TaskStateChangeCallback]) – much like the on_failure_callback except that it is executed when the task succeeds.
    # on_skipped_callback (None | airflow.models.abstractoperator.TaskStateChangeCallback | list[airflow.models.abstractoperator.TaskStateChangeCallback]) – much like the on_failure_callback except that it is executed when skipped occur; this callback will be called only if AirflowSkipException get raised. Explicitly it is NOT called if a task is not started to be executed because of a preceding branching decision in the DAG or a trigger rule which causes execution to skip so that the task execution is never scheduled.
    # pre_execute (TaskPreExecuteHook | None) – a function to be called immediately before task execution, receiving a context dictionary; raising an exception will prevent the task from being executed.
    # post_execute (TaskPostExecuteHook | None) – a function to be called immediately after task execution, receiving a context dictionary and task result; raising an exception will prevent the task from succeeding.
    trigger_rule: Optional[TriggerRule] = Field(
        default=None, description="defines the rule by which dependencies are applied for the task to get triggered."
    )
    # resources (dict[str, Any] | None) – A map of resource parameter names (the argument names of the Resources constructor) to their values.
    # run_as_user: Optional[str] = Field(default=None, description="unix username to impersonate while running the task")
    max_active_tis_per_dag: Optional[int] = Field(
        default=None, description="When set, a task will be able to limit the concurrent runs across execution_dates."
    )
    max_active_tis_per_dagrun: Optional[int] = Field(
        default=None, description="When set, a task will be able to limit the concurrent task instances per DAG run."
    )
    # executor_config (dict | None) – Additional task-level configuration parameters that are interpreted by a specific executor. Parameters are namespaced by the name of executor.
    do_xcom_push: Optional[bool] = Field(default=None, description="if True, an XCom is pushed containing the Operator’s result")
    multiple_outputs: Optional[bool] = Field(
        default=None,
        description="if True and do_xcom_push is True, pushes multiple XComs, one for each key in the returned dictionary result. If False and do_xcom_push is True, pushes a single XCom.",
    )
    # task_group (airflow.utils.task_group.TaskGroup | None) – The TaskGroup to which the task should belong. This is typically provided when not using a TaskGroup as a context manager.
    # doc (str | None) – Add documentation or notes to your Task objects that is visible in Task Instance details View in the Webserver
    doc: Optional[str] = Field(
        default=None,
        description="Add documentation or notes to your Task objects that is visible in Task Instance details View in the Webserver. This is a generic field that can be used for any format, but it is recommended to use specific fields for structured formats like Markdown, RST, JSON, or YAML.",
    )
    doc_md: Optional[str] = Field(
        default=None,
        description="Add documentation in Markdown format or notes to your Task objects that is visible in Task Instance details View in the Webserver.",
    )
    doc_rst: Optional[str] = Field(
        default=None,
        description="Add documentation in RST format or notes to your Task objects that is visible in Task Instance details View in the Webserver.",
    )
    doc_json: Optional[str] = Field(
        default=None,
        description="Add documentation in JSON format or notes to your Task objects that is visible in Task Instance details View in the Webserver.",
    )
    doc_yaml: Optional[str] = Field(
        default=None,
        description="Add documentation in YAML format or notes to your Task objects that is visible in Task Instance details View in the Webserver.",
    )
    task_display_name: Optional[str] = Field(default=None, description="The display name of the task which appears on the UI.")
    # logger_name (str | None) – Name of the logger used by the Operator to emit logs. If set to None (default), the logger name will fall back to airflow.task.operators.{class.__module__}.{class.__name__} (e.g. SimpleHttpOperator will have airflow.task.operators.airflow.providers.http.operators.http.SimpleHttpOperator as logger).
    # allow_nested_operators (bool) – if True, when an operator is executed within another one a warning message will be logged. If False, then an exception will be raised if the operator is badly used (e.g. nested within another one). In future releases of Airflow this parameter will be removed and an exception will always be thrown when operators are nested within each other (default is True).


TaskReference = Union[str, "Task"]
TaskAttribute = Union[str, Tuple[str, str], Tuple["Task", str]]


class Task(TaskArgs, TaskRenderMixin, TaskInstantiateMixin, extra="allow", validate_assignment=True):
    task_id: Optional[str] = Field(default=None, description="a unique, meaningful id for the task")

    operator: ImportPath = Field(description="airflow operator path")
    dependencies: Optional[Union[List[TaskReference], List[TaskAttribute]]] = Field(default=None, description="dependencies")

    @field_validator("dependencies", mode="before")
    def _validate_dependencies(cls, v):
        """Normalize to List[Union[str, Tuple[str, str]]]"""
        if isinstance(v, str):
            v = [v]
        if isinstance(v, tuple):
            v = [v]
        if isinstance(v, Task):
            # NOTE: keep as task for now in case period in task_id
            v = [v]

        for i, element in enumerate(v):
            if isinstance(element, tuple):
                assert len(element) == 2, "Tuple dependencies must be of length 2 (task_id, attribute)"
                if isinstance(element[0], Task):
                    # If the first element is a Task, convert to string representation
                    element = (element[0].task_id, element[1])
                if not isinstance(element[1], str):
                    raise ValueError("The second element of the tuple must be a string representing the attribute name.")
            if isinstance(element, str) and "." in element:
                # If the string contains a period, it is likely a task reference
                task_id, attribute = element.rsplit(".", 1)
                element = (task_id, attribute)
            if isinstance(element, Task):
                # If the element is a Task, convert to its task_id
                element = element.task_id
            v[i] = element
        return v

    def __lshift__(self, other: Union["Task", List["Task"]]) -> "Task":
        """e.g. a << Task() << b"""
        if isinstance(other, Task):
            self.dependencies = self.dependencies or []
            self.dependencies.append(other.task_id)
        elif isinstance(other, List):
            for task in other:
                self.__lshift__(task)
        return self

    def __rshift__(self, other: Union["Task", List["Task"]]) -> "Task":
        """e.g. a >> Task() >> b"""
        if isinstance(other, Task):
            other.dependencies = other.dependencies or []
            other.dependencies.append(self.task_id)
        elif isinstance(other, List):
            for task in other:
                self.__rshift__(task)
        return self

    def set_upstream(self, other: Union["Task", List["Task"]]):
        if isinstance(other, Task):
            self.dependencies = self.dependencies or []
            self.dependencies.append(other.task_id)
        elif isinstance(other, List):
            for task in other:
                self.set_upstream(task)
        return self

    def set_downstream(self, other: Union["Task", List["Task"]]):
        if isinstance(other, Task):
            other.dependencies = other.dependencies or []
            other.dependencies.append(self.task_id)
        elif isinstance(other, List):
            for task in other:
                self.set_downstream(task)
        return self


__all_task_fields__ = list(Task.__pydantic_fields__.keys() if hasattr(Task, "__pydantic_fields__") else Task.__fields__.keys())

# Alias
TaskModel = Task
