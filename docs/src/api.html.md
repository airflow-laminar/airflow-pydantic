# API reference

This reference is generated from public model signatures and docstrings.

## Core models

### *pydantic model* airflow_pydantic.core.base.BaseModel

Bases: `BaseModel`

#### model_dump(\*\*kwargs)

!!! abstract “Usage Documentation”
: [model_dump](../concepts/serialization.md#modelmodel_dump)

Generate a dictionary representation of the model, optionally specifying which fields to include or exclude.

* **Parameters:**
  * **mode** – The mode in which to_python should run.
    If mode is ‘json’, the output will only contain JSON serializable types.
    If mode is ‘python’, the output may contain non-JSON-serializable Python objects.
  * **include** – A set of fields to include in the output.
  * **exclude** – A set of fields to exclude from the output.
  * **context** – Additional context to pass to the serializer.
  * **by_alias** – Whether to use the field’s alias in the dictionary key if defined.
  * **exclude_unset** – Whether to exclude fields that have not been explicitly set.
  * **exclude_defaults** – Whether to exclude fields that are set to their default value.
  * **exclude_none** – Whether to exclude fields that have a value of None.
  * **round_trip** – If True, dumped values should be valid as input for non-idempotent types such as Json[T].
  * **warnings** – How to handle serialization errors. False/”none” ignores them, True/”warn” logs errors,
    “error” raises a [PydanticSerializationError][pydantic_core.PydanticSerializationError].
  * **fallback** – A function to call when an unknown value is encountered. If not provided,
    a [PydanticSerializationError][pydantic_core.PydanticSerializationError] error is raised.
  * **serialize_as_any** – Whether to serialize fields with duck-typing serialization behavior.
* **Returns:**
  A dictionary representation of the model.

#### model_dump_json(\*\*kwargs)

!!! abstract “Usage Documentation”
: [model_dump_json](../concepts/serialization.md#modelmodel_dump_json)

Generates a JSON representation of the model using Pydantic’s to_json method.

* **Parameters:**
  * **indent** – Indentation to use in the JSON output. If None is passed, the output will be compact.
  * **include** – Field(s) to include in the JSON output.
  * **exclude** – Field(s) to exclude from the JSON output.
  * **context** – Additional context to pass to the serializer.
  * **by_alias** – Whether to serialize using field aliases.
  * **exclude_unset** – Whether to exclude fields that have not been explicitly set.
  * **exclude_defaults** – Whether to exclude fields that are set to their default value.
  * **exclude_none** – Whether to exclude fields that have a value of None.
  * **round_trip** – If True, dumped values should be valid as input for non-idempotent types such as Json[T].
  * **warnings** – How to handle serialization errors. False/”none” ignores them, True/”warn” logs errors,
    “error” raises a [PydanticSerializationError][pydantic_core.PydanticSerializationError].
  * **fallback** – A function to call when an unknown value is encountered. If not provided,
    a [PydanticSerializationError][pydantic_core.PydanticSerializationError] error is raised.
  * **serialize_as_any** – Whether to serialize fields with duck-typing serialization behavior.
* **Returns:**
  A JSON string representation of the model.

<a id="module-airflow_pydantic.core.dag"></a>

### *pydantic model* airflow_pydantic.core.dag.Dag

Bases: [`DagArgs`](#airflow_pydantic.core.dag.DagArgs), `DagRenderMixin`, `DagInstantiateMixin`

#### *field* dag_id *: str | None* *= None*

The id of the DAG; must consist exclusively of alphanumeric characters, dashes, dots and underscores (all ASCII)

#### *field* tasks *: dict[str, Annotated[[Task](#airflow_pydantic.core.task.Task), SerializeAsAny()]] | None* *[Optional]*

List of tasks in the DAG

### *pydantic model* airflow_pydantic.core.dag.DagArgs

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* description *: str | None* *= None*

The description for the DAG to e.g. be shown on the webserver

#### *field* schedule *: timedelta | Literal['NOTSET'] | str | None | [CronDataIntervalTimetable](#airflow_pydantic.utils.timetables.CronDataIntervalTimetable) | [CronTriggerTimetable](#airflow_pydantic.utils.timetables.CronTriggerTimetable) | [MultipleCronTriggerTimetable](#airflow_pydantic.utils.timetables.MultipleCronTriggerTimetable) | [EventsTimetable](#airflow_pydantic.utils.timetables.EventsTimetable) | [DeltaDataIntervalTimetable](#airflow_pydantic.utils.timetables.DeltaDataIntervalTimetable) | [DeltaTriggerTimetable](#airflow_pydantic.utils.timetables.DeltaTriggerTimetable)* *= None*

Defines the rules according to which DAG runs are scheduled. Can accept cron string, timedelta object, Timetable, or list of Dataset objects. If this is not provided, the DAG will be set to the default schedule timedelta(days=1). See also Customizing DAG Scheduling with Timetables.

#### *field* start_date *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The timestamp from which the scheduler will attempt to backfill

#### *field* end_date *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

A date beyond which your DAG won’t run, leave to None for open-ended scheduling

#### *field* default_args *: [TaskArgs](#airflow_pydantic.core.task.TaskArgs) | None* *= None*

Default arguments for tasks in the DAG

#### *field* max_active_tasks *: int | None* *= None*

the number of task instances allowed to run concurrently

#### *field* max_active_runs *: int | None* *= None*

maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won’t create new active DAG runs

#### *field* default_view *: Literal['grid', 'graph', 'duration', 'gantt', 'landing_times'] | None* *= None*

Specify DAG default view (grid, graph, duration, gantt, landing_times), default grid

#### *field* orientation *: Literal['LR', 'TB', 'RL', 'BT'] | None* *= None*

Specify DAG orientation in graph view (LR, TB, RL, BT), default LR

#### *field* catchup *: bool | None* *= None*

Perform scheduler catchup (or only run latest)? Defaults to False

#### *field* doc_md *: str | None* *= None*

Markdown formatted documentation for the DAG. This will be rendered in the UI.

#### *field* params *: dict[str, Annotated[Param, [ParamType](#airflow_pydantic.utils.param.ParamType)]] | None* *= None*

A dictionary of DAG-level parameters that are made accessible in templates, namespaced under params. These params can be overridden at the task level.

#### *field* is_paused_upon_creation *: bool | None* *= None*

Specifies if the dag is paused when created for the first time. If the dag exists already, this flag will be ignored.

#### *field* tags *: list[str] | None* *= None*

List of tags to help filtering DAGs in the UI.

#### *field* dag_display_name *: str | None* *= None*

The display name of the DAG which appears on the UI.

#### *field* enabled *: bool | None* *= None*

Whether the DAG is enabled

### airflow_pydantic.core.dag.DagModel

alias of [`Dag`](#airflow_pydantic.core.dag.Dag)

<a id="module-airflow_pydantic.core.task"></a>

### *pydantic model* airflow_pydantic.core.task.Task

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs), `TaskRenderMixin`, `TaskInstantiateMixin`

#### *field* task_id *: str | None* *= None*

a unique, meaningful id for the task

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *[Required]*

airflow operator path

#### *field* dependencies *: list[str | [Task](#airflow_pydantic.core.task.Task)] | list[str | tuple[str, str] | tuple[[Task](#airflow_pydantic.core.task.Task), str]] | None* *= None*

dependencies

#### set_upstream(other: [Task](#airflow_pydantic.core.task.Task) | list[[Task](#airflow_pydantic.core.task.Task)])

#### set_downstream(other: [Task](#airflow_pydantic.core.task.Task) | list[[Task](#airflow_pydantic.core.task.Task)])

### *pydantic model* airflow_pydantic.core.task.TaskArgs

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* owner *: str | None* *= None*

the owner of the task. Using a meaningful description (e.g. user/person/team/role name) to clarify ownership is recommended.

#### *field* email *: list[str] | None* *= None*

the ‘to’ email address(es) used in email alerts

#### *field* email_on_failure *: bool | None* *= None*

Indicates whether email alerts should be sent when a task failed

#### *field* email_on_retry *: bool | None* *= None*

Indicates whether email alerts should be sent when a task is retried

#### *field* retries *: int | None* *= None*

the number of retries that should be performed before failing the task

#### *field* retry_delay *: timedelta | None* *= None*

delay between retries

#### *field* retry_exponential_backoff *: bool | None* *= None*

allow progressively longer waits between retries by using exponential backoff algorithm on retry delay (delay will be converted into seconds)

#### *field* max_retry_delay *: timedelta | None* *= None*

maximum delay interval between retries

#### *field* start_date *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The start_date for the task, determines the execution_date for the first task instance. The best practice is to have the start_date rounded to your DAG’s schedule_interval. Daily jobs have their start_date some day at 00:00:00, hourly jobs have their start_date at 00:00 of a specific hour. Note that Airflow simply looks at the latest execution_date and adds the schedule_interval to determine the next execution_date. It is also very important to note that different tasks’ dependencies need to line up in time. If task A depends on task B and their start_date are offset in a way that their execution_date don’t line up, A’s dependencies will never be met. If you are looking to delay a task, for example running a daily task at 2AM, look into the TimeSensor and TimeDeltaSensor. We advise against using dynamic start_date and recommend using fixed ones. Read the FAQ entry about start_date for more information.

#### *field* end_date *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

if specified, the scheduler won’t go beyond this date

#### *field* depends_on_past *: bool | None* *= None*

when set to true, task instances will run sequentially and only if the previous instance has succeeded or has been skipped. The task instance for the start_date is allowed to run.

#### *field* queue *: str | None* *= None*

which queue to target when running this job. Not all executors implement queue management, the CeleryExecutor does support targeting specific queues.

#### *field* pool *: str | [Pool](#airflow_pydantic.utils.pool.Pool) | None* *= None*

the slot pool this task should run in, slot pools are a way to limit concurrency for certain tasks

#### *field* pool_slots *: int | None* *= None*

the number of pool slots this task should use (>= 1) Values less than 1 are not allowed

#### *field* execution_timeout *: timedelta | None* *= None*

max time allowed for the execution of this task instance, if it goes beyond it will raise and fail.

#### *field* trigger_rule *: [TriggerRule](#airflow_pydantic.utils.common.TriggerRule) | None* *= None*

defines the rule by which dependencies are applied for the task to get triggered.

#### *field* max_active_tis_per_dag *: int | None* *= None*

When set, a task will be able to limit the concurrent runs across execution_dates.

#### *field* max_active_tis_per_dagrun *: int | None* *= None*

When set, a task will be able to limit the concurrent task instances per DAG run.

#### *field* do_xcom_push *: bool | None* *= None*

if True, an XCom is pushed containing the Operator’s result

#### *field* multiple_outputs *: bool | None* *= None*

if True and do_xcom_push is True, pushes multiple XComs, one for each key in the returned dictionary result. If False and do_xcom_push is True, pushes a single XCom.

#### *field* doc *: str | None* *= None*

Add documentation or notes to your Task objects that is visible in Task Instance details View in the Webserver. This is a generic field that can be used for any format, but it is recommended to use specific fields for structured formats like Markdown, RST, JSON, or YAML.

#### *field* doc_md *: str | None* *= None*

Add documentation in Markdown format or notes to your Task objects that is visible in Task Instance details View in the Webserver.

#### *field* doc_rst *: str | None* *= None*

Add documentation in RST format or notes to your Task objects that is visible in Task Instance details View in the Webserver.

#### *field* doc_json *: str | None* *= None*

Add documentation in JSON format or notes to your Task objects that is visible in Task Instance details View in the Webserver.

#### *field* doc_yaml *: str | None* *= None*

Add documentation in YAML format or notes to your Task objects that is visible in Task Instance details View in the Webserver.

#### *field* task_display_name *: str | None* *= None*

The display name of the task which appears on the UI.

### airflow_pydantic.core.task.TaskModel

alias of [`Task`](#airflow_pydantic.core.task.Task)

## Operators

### airflow_pydantic.operators.bash.BashOperator

alias of [`BashTask`](#airflow_pydantic.operators.bash.BashTask)

### airflow_pydantic.operators.bash.BashOperatorArgs

alias of [`BashTaskArgs`](#airflow_pydantic.operators.bash.BashTaskArgs)

### *pydantic model* airflow_pydantic.operators.bash.BashTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BashTaskArgs`](#airflow_pydantic.operators.bash.BashTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BashOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.bash.BashTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* bash_command *: str | list[str] | [BashCommands](#airflow_pydantic.utils.bash.BashCommands)* *= None*

bash command string, list of strings, or model

#### *field* env *: dict[str, str] | None* *= None*

#### *field* append_env *: bool | None* *= None*

Append environment variables to the existing environment. Default is False

#### *field* output_encoding *: str | None* *= None*

Output encoding for the command, default is ‘utf-8’

#### *field* skip_on_exit_code *: int | None* *= None*

Exit code to skip on, default is 99

#### *field* cwd *: str | None* *= None*

#### *field* output_processor *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | None* *= None*

<a id="module-airflow_pydantic.operators.datetime"></a>

### airflow_pydantic.operators.datetime.BranchDateTimeOperator

alias of [`BranchDateTimeTask`](#airflow_pydantic.operators.datetime.BranchDateTimeTask)

### airflow_pydantic.operators.datetime.BranchDateTimeOperatorArgs

alias of [`BranchDateTimeTaskArgs`](#airflow_pydantic.operators.datetime.BranchDateTimeTaskArgs)

### *pydantic model* airflow_pydantic.operators.datetime.BranchDateTimeTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BranchDateTimeTaskArgs`](#airflow_pydantic.operators.datetime.BranchDateTimeTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BranchDateTimeOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.datetime.BranchDateTimeTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* follow_task_ids_if_true *: list[str] | None* *= None*

List of task IDs to follow if condition evaluates to True

#### *field* follow_task_ids_if_false *: list[str] | None* *= None*

List of task IDs to follow if condition evaluates to False

#### *field* target_lower *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The lower bound datetime to compare against

#### *field* target_upper *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The upper bound datetime to compare against

#### *field* use_task_logical_date *: bool | None* *= None*

If True, uses the task’s logical date for comparison; otherwise, uses the current datetime

#### *field* use_task_execution_date *: bool | None* *= None*

If True, uses the task’s execution date for comparison; otherwise, uses the current datetime

<a id="module-airflow_pydantic.operators.email"></a>

### airflow_pydantic.operators.email.EmailOperator

alias of [`EmailTask`](#airflow_pydantic.operators.email.EmailTask)

### airflow_pydantic.operators.email.EmailOperatorArgs

alias of [`EmailTaskArgs`](#airflow_pydantic.operators.email.EmailTaskArgs)

### *pydantic model* airflow_pydantic.operators.email.EmailTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`EmailTaskArgs`](#airflow_pydantic.operators.email.EmailTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.EmailOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.email.EmailTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* to *: str | list[str]* *[Required]*

List of emails to send the email to. (templated)

#### *field* subject *: str* *[Required]*

Subject line for the email. (templated)

#### *field* html_content *: str* *[Required]*

Content of the email, html markup is allowed. (templated)

#### *field* files *: list[str] | None* *= None*

File names to attach in email. (templated)

#### *field* cc *: str | list[str] | None* *= None*

List of recipients to be added in CC field. (templated)

#### *field* bcc *: str | list[str] | None* *= None*

List of recipients to be added in BCC field. (templated)

#### *field* mime_subtype *: str | None* *= None*

MIME sub content type, default is ‘mixed’

#### *field* mime_charset *: str | None* *= None*

Character set parameter added to the Content-Type header, default is ‘us-ascii’

#### *field* conn_id *: str | None* *= None*

The connection to use for sending the email.

#### *field* custom_headers *: dict[str, str] | None* *= None*

Additional headers to be added to the MIME message.

<a id="module-airflow_pydantic.operators.empty"></a>

### airflow_pydantic.operators.empty.EmptyOperator

alias of [`EmptyTask`](#airflow_pydantic.operators.empty.EmptyTask)

### airflow_pydantic.operators.empty.EmptyOperatorArgs

alias of `EmptyTaskArgs`

### *pydantic model* airflow_pydantic.operators.empty.EmptyTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), `EmptyTaskArgs`

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.EmptyOperator'*

airflow operator path

<a id="module-airflow_pydantic.operators.external_task"></a>

### *pydantic model* airflow_pydantic.operators.external_task.ExternalTaskMarker

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`ExternalTaskMarkerArgs`](#airflow_pydantic.operators.external_task.ExternalTaskMarkerArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.ExternalTaskMarker'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.external_task.ExternalTaskMarkerArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

<a id="module-airflow_pydantic.operators.hitl"></a>

### airflow_pydantic.operators.hitl.ApprovalOperator

alias of [`ApprovalTask`](#airflow_pydantic.operators.hitl.ApprovalTask)

### airflow_pydantic.operators.hitl.ApprovalOperatorArgs

alias of [`ApprovalTaskArgs`](#airflow_pydantic.operators.hitl.ApprovalTaskArgs)

### *pydantic model* airflow_pydantic.operators.hitl.ApprovalTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`ApprovalTaskArgs`](#airflow_pydantic.operators.hitl.ApprovalTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.ApprovalOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.hitl.ApprovalTaskArgs

Bases: [`HITLTaskArgs`](#airflow_pydantic.operators.hitl.HITLTaskArgs)

### airflow_pydantic.operators.hitl.HITLBranchOperator

alias of [`HITLBranchTask`](#airflow_pydantic.operators.hitl.HITLBranchTask)

### airflow_pydantic.operators.hitl.HITLBranchOperatorArgs

alias of [`HITLBranchTaskArgs`](#airflow_pydantic.operators.hitl.HITLBranchTaskArgs)

### *pydantic model* airflow_pydantic.operators.hitl.HITLBranchTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`HITLBranchTaskArgs`](#airflow_pydantic.operators.hitl.HITLBranchTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.HITLBranchOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.hitl.HITLBranchTaskArgs

Bases: [`HITLTaskArgs`](#airflow_pydantic.operators.hitl.HITLTaskArgs)

### airflow_pydantic.operators.hitl.HITLEntryOperator

alias of [`HITLEntryTask`](#airflow_pydantic.operators.hitl.HITLEntryTask)

### airflow_pydantic.operators.hitl.HITLEntryOperatorArgs

alias of [`HITLEntryTaskArgs`](#airflow_pydantic.operators.hitl.HITLEntryTaskArgs)

### *pydantic model* airflow_pydantic.operators.hitl.HITLEntryTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`HITLEntryTaskArgs`](#airflow_pydantic.operators.hitl.HITLEntryTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.HITLEntryOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.hitl.HITLEntryTaskArgs

Bases: [`HITLTaskArgs`](#airflow_pydantic.operators.hitl.HITLTaskArgs)

### airflow_pydantic.operators.hitl.HITLOperator

alias of [`HITLTask`](#airflow_pydantic.operators.hitl.HITLTask)

### airflow_pydantic.operators.hitl.HITLOperatorArgs

alias of [`HITLTaskArgs`](#airflow_pydantic.operators.hitl.HITLTaskArgs)

### *pydantic model* airflow_pydantic.operators.hitl.HITLTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`HITLTaskArgs`](#airflow_pydantic.operators.hitl.HITLTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.HITLOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.hitl.HITLTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* subject *: str | None* *= None*

Headline/subject presented to the user for the interaction task

#### *field* options *: list[str] | None* *= None*

List of options that the an user can select from to complete the task.

#### *field* body *: str | None* *= None*

Descriptive text (with Markdown support) that gives the details that are needed to decide.

#### *field* defaults *: list[str] | None* *= None*

The default options and the options that are taken if timeout is passed.

#### *field* multiple *: bool | None* *= None*

Whether the user can select one or multiple options.

#### *field* params *: dict[str, Annotated[Param, [ParamType](#airflow_pydantic.utils.param.ParamType)]] | None* *= None*

dictionary of parameter definitions that are in the format of Dag params such that a Form Field can be rendered. Entered data is validated (schema, required fields) like for a Dag run and added to XCom of the task result.

<a id="module-airflow_pydantic.operators.latest_only"></a>

<a id="module-airflow_pydantic.operators.python"></a>

### airflow_pydantic.operators.python.BranchExternalPythonOperator

alias of [`BranchExternalPythonTask`](#airflow_pydantic.operators.python.BranchExternalPythonTask)

### airflow_pydantic.operators.python.BranchExternalPythonOperatorArgs

alias of [`BranchExternalPythonTaskArgs`](#airflow_pydantic.operators.python.BranchExternalPythonTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.BranchExternalPythonTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BranchExternalPythonTaskArgs`](#airflow_pydantic.operators.python.BranchExternalPythonTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BranchExternalPythonOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.BranchExternalPythonTaskArgs

Bases: [`ExternalPythonTaskArgs`](#airflow_pydantic.operators.python.ExternalPythonTaskArgs)

### airflow_pydantic.operators.python.BranchPythonOperator

alias of [`BranchPythonTask`](#airflow_pydantic.operators.python.BranchPythonTask)

### airflow_pydantic.operators.python.BranchPythonOperatorArgs

alias of [`BranchPythonTaskArgs`](#airflow_pydantic.operators.python.BranchPythonTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.BranchPythonTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BranchPythonTaskArgs`](#airflow_pydantic.operators.python.BranchPythonTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BranchPythonOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.BranchPythonTaskArgs

Bases: [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

### airflow_pydantic.operators.python.BranchPythonVirtualenvOperator

alias of [`BranchPythonVirtualenvTask`](#airflow_pydantic.operators.python.BranchPythonVirtualenvTask)

### airflow_pydantic.operators.python.BranchPythonVirtualenvOperatorArgs

alias of [`BranchPythonVirtualenvTaskArgs`](#airflow_pydantic.operators.python.BranchPythonVirtualenvTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.BranchPythonVirtualenvTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BranchPythonVirtualenvTaskArgs`](#airflow_pydantic.operators.python.BranchPythonVirtualenvTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BranchPythonVirtualenvOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.BranchPythonVirtualenvTaskArgs

Bases: [`PythonVirtualenvTaskArgs`](#airflow_pydantic.operators.python.PythonVirtualenvTaskArgs)

### airflow_pydantic.operators.python.ExternalPythonOperator

alias of [`ExternalPythonTask`](#airflow_pydantic.operators.python.ExternalPythonTask)

### airflow_pydantic.operators.python.ExternalPythonOperatorArgs

alias of [`ExternalPythonTaskArgs`](#airflow_pydantic.operators.python.ExternalPythonTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.ExternalPythonTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`ExternalPythonTaskArgs`](#airflow_pydantic.operators.python.ExternalPythonTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.ExternalPythonOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.ExternalPythonTaskArgs

Bases: [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

#### *field* python *: str* *[Required]*

Full path string (file-system specific) that points to a Python binary inside a virtual environment that should be used (in VENV/bin folder). Should be absolute path (so usually start with “/” or “X:/” depending on the filesystem/os used).

#### *field* python_callable *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *[Required]*

A python function with no references to outside variables, defined with def, which will be run in a virtual environment.

#### *field* serializer *: Literal['pickle', 'cloudpickle', 'dill'] | None* *= None*

Which serializer use to serialize the args and result. It can be one of the following: ‘pickle’ (default) Use pickle for serialization. Included in the Python Standard Library.; ‘cloudpickle’ Use cloudpickle for serialize more complex types, this requires to include cloudpickle in your requirements.; ‘dill’ Use dill for serialize more complex types, this requires to include dill in your requirements.

#### *field* op_args *: list[Any] | None* *= None*

A list of positional arguments to pass to python_callable.

#### *field* op_kwargs *: dict[str, Any] | None* *= None*

A dict of keyword arguments to pass to python_callable.

#### *field* string_args *: list[str] | None* *= None*

Strings that are present in the global var external_python_string_args, available to python_callable at runtime as a list[str]. Note that args are split by newline.

#### *field* templates_dict *: dict[str, Any] | None* *= None*

a dictionary where the values are templates that will get templated by the Airflow engine sometime between \_\_init_\_ and execute takes place and are made available in your callable’s context after the template has been applied

#### *field* templates_exts *: list[str] | None* *= None*

a list of file extensions to resolve while processing templated fields, for examples [‘.sql’, ‘.hql’]

#### *field* expect_airflow *: bool | None* *= None*

expect Airflow to be installed in the target environment. If true, the operator will raise warning if Airflow is not installed, and it will attempt to load Airflow macros when starting.

#### *field* skip_on_exit_code *: int | list[int] | None* *= None*

If python_callable exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.

#### *field* env_vars *: dict[str, str] | None* *= None*

A dictionary containing additional environment variables to set for the external python environment when it is executed.

#### *field* inherit_env *: bool | None* *= None*

Whether to inherit the current environment variables when executing the external python. If set to True, the external python will inherit the environment variables of the parent process (os.environ). If set to False, the external python will be executed with a clean environment.

### airflow_pydantic.operators.python.PythonOperator

alias of [`PythonTask`](#airflow_pydantic.operators.python.PythonTask)

### airflow_pydantic.operators.python.PythonOperatorArgs

alias of [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.PythonTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.PythonOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.PythonTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* python_callable *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= None*

python_callable

#### *field* op_args *: list[object] | None* *= None*

a list of positional arguments that will get unpacked when calling your callable

#### *field* op_kwargs *: dict[str, object] | None* *= None*

a dictionary of keyword arguments that will get unpacked in your function

#### *field* templates_dict *: dict[str, object] | None* *= None*

a dictionary where the values are templates that will get templated by the Airflow engine sometime between \_\_init_\_ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)

#### *field* templates_exts *: list[str] | None* *= None*

a list of file extensions to resolve while processing templated fields, for examples [‘.sql’, ‘.hql’]

#### *field* show_return_value_in_logs *: bool | None* *= None*

a bool value whether to show return_value logs. Defaults to True, which allows return value log output. It can be set to False

### airflow_pydantic.operators.python.PythonVirtualenvOperator

alias of [`PythonVirtualenvTask`](#airflow_pydantic.operators.python.PythonVirtualenvTask)

### airflow_pydantic.operators.python.PythonVirtualenvOperatorArgs

alias of [`PythonVirtualenvTaskArgs`](#airflow_pydantic.operators.python.PythonVirtualenvTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.PythonVirtualenvTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`PythonVirtualenvTaskArgs`](#airflow_pydantic.operators.python.PythonVirtualenvTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.PythonVirtualenvOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.PythonVirtualenvTaskArgs

Bases: [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

#### *field* python_callable *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *[Required]*

A python function with no references to outside variables, defined with def, which will be run in a virtual environment.

#### *field* requirements *: list[str] | None* *= None*

Either a list of requirement strings, or a (templated) “requirements file” as specified by pip.

#### *field* python_version *: str | None* *= None*

The Python version to run the virtual environment with. Note that both 2 and 2.7 are acceptable forms.

#### *field* serializer *: Literal['pickle', 'cloudpickle', 'dill']* *= 'pickle'*

Which serializer use to serialize the args and result.

#### *field* system_site_packages *: bool | None* *= None*

Whether to include system_site_packages in your virtual environment. See virtualenv documentation for more information.

#### *field* pip_install_options *: list[str] | None* *= None*

a list of pip install options when installing requirements See ‘pip install -h’ for available options

#### *field* op_args *: list[Any] | None* *= None*

A list of positional arguments to pass to python_callable.

#### *field* op_kwargs *: dict[str, Any] | None* *= None*

A dict of keyword arguments to pass to python_callable.

#### *field* string_args *: list[str] | None* *= None*

Strings that are present in the global var virtualenv_string_args, available to python_callable at runtime as a list[str]. Note that args are split by newline.

#### *field* templates_dict *: dict[str, Any] | None* *= None*

a dictionary where the values are templates that will get templated by the Airflow engine sometime between \_\_init_\_ and execute takes place and are made available in your callable’s context after the template has been applied

#### *field* templates_exts *: list[str] | None* *= None*

a list of file extensions to resolve while processing templated fields, for examples [‘.sql’, ‘.hql’]

#### *field* expect_airflow *: bool | None* *= None*

expect Airflow to be installed in the target environment. If true, the operator will raise warning if Airflow is not installed, and it will attempt to load Airflow macros when starting.

#### *field* skip_on_exit_code *: int | list[int] | None* *= None*

If python_callable exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.

#### *field* index_urls *: list[str] | str | None* *= None*

an optional list of index urls to load Python packages from. If not provided the system pip conf will be used to source packages from.

#### *field* index_urls_from_connection_ids *: list[str] | str | None* *= None*

An optional list of PackageIndex connection IDs. Will be appended to index_urls.

#### *field* venv_cache_path *: str | None* *= None*

Optional path to the virtual environment parent folder in which the virtual environment will be cached, creates a sub-folder venv-{hash} whereas hash will be replaced with a checksum of requirements. If not provided the virtual environment will be created and deleted in a temp folder for every execution.

#### *field* env_vars *: dict[str, str] | None* *= None*

A dictionary containing additional environment variables to set for the virtual environment when it is executed.

#### *field* inherit_env *: bool | None* *= None*

Whether to inherit the current environment variables when executing the virtual environment. If set to True, the virtual environment will inherit the environment variables of the parent process (os.environ). If set to False, the virtual environment will be executed with a clean environment.

### airflow_pydantic.operators.python.ShortCircuitOperator

alias of [`ShortCircuitTask`](#airflow_pydantic.operators.python.ShortCircuitTask)

### airflow_pydantic.operators.python.ShortCircuitOperatorArgs

alias of [`ShortCircuitTaskArgs`](#airflow_pydantic.operators.python.ShortCircuitTaskArgs)

### *pydantic model* airflow_pydantic.operators.python.ShortCircuitTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`ShortCircuitTaskArgs`](#airflow_pydantic.operators.python.ShortCircuitTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.ShortCircuitOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.python.ShortCircuitTaskArgs

Bases: [`PythonTaskArgs`](#airflow_pydantic.operators.python.PythonTaskArgs)

#### *field* ignore_downstream_trigger_rules *: bool | None* *= None*

If set to True, all downstream tasks from this operator task will be skipped. This is the default behavior. If set to False, the direct, downstream task(s) will be skipped but the trigger_rule defined for a other downstream tasks will be respected.

<a id="module-airflow_pydantic.operators.smooth"></a>

<a id="module-airflow_pydantic.operators.ssh"></a>

### airflow_pydantic.operators.ssh.SSHOperator

alias of [`SSHTask`](#airflow_pydantic.operators.ssh.SSHTask)

### airflow_pydantic.operators.ssh.SSHOperatorArgs

alias of [`SSHTaskArgs`](#airflow_pydantic.operators.ssh.SSHTaskArgs)

### *pydantic model* airflow_pydantic.operators.ssh.SSHTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`SSHTaskArgs`](#airflow_pydantic.operators.ssh.SSHTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.SSHOperator'*

airflow operator path

#### instantiate(\*\*kwargs)

#### render(raw: bool = False, dag_from_context: bool = False, airflow_major_version: int = 2, \*\*kwargs)

### *pydantic model* airflow_pydantic.operators.ssh.SSHTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* ssh_hook *: Annotated[SSHHook, SSHHookType] | Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | [BalancerHostQueryConfiguration](#airflow_pydantic.extras.balancer.query.BalancerHostQueryConfiguration) | [Host](#airflow_pydantic.extras.balancer.host.Host) | None* *= None*

predefined ssh_hook to use for remote execution. Either ssh_hook or ssh_conn_id needs to be provided.

#### *field* ssh_hook_host *: [Host](#airflow_pydantic.extras.balancer.host.Host) | None* *= None*

#### *field* ssh_hook_hosts *: list[[Host](#airflow_pydantic.extras.balancer.host.Host)] | None* *= None*

#### *field* ssh_hook_foo *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | None* *= None*

#### *field* ssh_hook_external *: bool | None* *= False*

Whether to force the ssh_hook to be an external call or not. Only works when ssh_hook is a Callable

#### *field* ssh_conn_id *: str | None* *= None*

ssh connection id from airflow Connections. ssh_conn_id will be ignored if ssh_hook is provided.

#### *field* remote_host *: str | None* *= None*

remote host to connect (templated) Nullable. If provided, it will replace the remote_host which was defined in ssh_hook or predefined in the connection of ssh_conn_id.

#### *field* command *: str | list[str] | [BashCommands](#airflow_pydantic.utils.bash.BashCommands)* *= None*

command to execute on remote host. (templated)

#### *field* conn_timeout *: int | None* *= None*

timeout (in seconds) for maintaining the connection. The default is 10 seconds. Nullable. If provided, it will replace the conn_timeout which was predefined in the connection of ssh_conn_id.

#### *field* cmd_timeout *: int | None* *= None*

timeout (in seconds) for executing the command. The default is 10 seconds. Nullable, None means no timeout. If provided, it will replace the cmd_timeout which was predefined in the connection of ssh_conn_id.

#### *field* environment *: dict[str, str] | None* *= None*

a dict of shell environment variables. Note that the server will reject them silently if AcceptEnv is not set in SSH config. (templated)

#### *field* get_pty *: bool | None* *= None*

request a pseudo-terminal from the server. Set to True to have the remote process killed upon task timeout. The default is False but note that get_pty is forced to True when the command starts with sudo.

#### *field* banner_timeout *: int | None* *= None*

timeout to wait for banner from the server in seconds

#### *field* skip_on_exit_code *: int | None* *= None*

If command exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.

<a id="module-airflow_pydantic.operators.trigger"></a>

### airflow_pydantic.operators.trigger.TriggerDagRunOperator

alias of [`TriggerDagRunTask`](#airflow_pydantic.operators.trigger.TriggerDagRunTask)

### airflow_pydantic.operators.trigger.TriggerDagRunOperatorArgs

alias of [`TriggerDagRunTaskArgs`](#airflow_pydantic.operators.trigger.TriggerDagRunTaskArgs)

### *pydantic model* airflow_pydantic.operators.trigger.TriggerDagRunTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`TriggerDagRunTaskArgs`](#airflow_pydantic.operators.trigger.TriggerDagRunTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.TriggerDagRunOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.trigger.TriggerDagRunTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* trigger_dag_id *: str* *[Required]*

The DAG ID of the DAG to trigger

#### *field* trigger_run_id *: str | None* *= None*

The run ID of the DAG run to trigger

#### *field* conf *: dict[str, Any] | None* *= None*

A dictionary of configuration parameters to pass to the triggered DAG run

#### *field* logical_date *: datetime | str | None* *= None*

The logical date of the DAG run to trigger

#### *field* reset_dag_run *: bool | None* *= None*

Whether clear existing DAG run if already exists. This is useful when backfill or rerun an existing DAG run. This only resets (not recreates) the DAG run. DAG run conf is immutable and will not be reset on rerun of an existing DAG run. When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised. When reset_dag_run=True and dag run exists, existing DAG run will be cleared to rerun.

#### *field* wait_for_completion *: bool | None* *= None*

Whether or not wait for DAG run completion.

#### *field* poke_interval *: int | None* *= None*

Poke interval to check DAG run status when wait_for_completion=True)

#### *field* allowed_states *: list[str] | None* *= None*

Optional list of allowed DAG run states of the triggered DAG. This is useful when setting wait_for_completion to True. Must be a valid DagRunState

#### *field* failed_states *: list[str] | None* *= None*

Optional list of failed or disallowed DAG run states of the triggered DAG. This is useful when setting wait_for_completion to True. Must be a valid DagRunState

#### *field* skip_when_already_exists *: bool | None* *= None*

Set to true to mark the task as SKIPPED if a DAG run of the triggered DAG for the same logical date already exists.

#### *field* deferrable *: bool | None* *= None*

If waiting for completion, whether or not to defer the task until done, default is False.

<a id="module-airflow_pydantic.operators.weekday"></a>

### airflow_pydantic.operators.weekday.BranchDayOfWeekOperator

alias of [`BranchDayOfWeekTask`](#airflow_pydantic.operators.weekday.BranchDayOfWeekTask)

### airflow_pydantic.operators.weekday.BranchDayOfWeekOperatorArgs

alias of [`BranchDayOfWeekTaskArgs`](#airflow_pydantic.operators.weekday.BranchDayOfWeekTaskArgs)

### *pydantic model* airflow_pydantic.operators.weekday.BranchDayOfWeekTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BranchDayOfWeekTaskArgs`](#airflow_pydantic.operators.weekday.BranchDayOfWeekTaskArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BranchDayOfWeekOperator'*

airflow operator path

### *pydantic model* airflow_pydantic.operators.weekday.BranchDayOfWeekTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* follow_task_ids_if_true *: list[str] | None* *= None*

List of task IDs to follow if condition evaluates to True

#### *field* follow_task_ids_if_false *: list[str] | None* *= None*

List of task IDs to follow if condition evaluates to False

#### *field* target_lower *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The lower bound datetime to compare against

#### *field* target_upper *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)] | None* *= None*

The upper bound datetime to compare against

#### *field* use_task_logical_date *: bool | None* *= None*

If True, uses the task’s logical date for comparison; otherwise, uses the current datetime

#### *field* use_task_execution_date *: bool | None* *= None*

If True, uses the task’s execution date for comparison; otherwise, uses the current datetime

## Sensors

### *pydantic model* airflow_pydantic.sensors.base.BaseSensorArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* poke_interval *: timedelta | float | None* *= None*

#### *field* timeout *: timedelta | float | None* *= None*

#### *field* soft_fail *: bool | None* *= None*

#### *field* mode *: Literal['poke', 'reschedule'] | None* *= None*

#### *field* exponential_backoff *: bool | None* *= None*

#### *field* max_wait *: timedelta | float | None* *= None*

#### *field* silent_fail *: bool | None* *= None*

#### *field* never_fail *: bool | None* *= None*

<a id="module-airflow_pydantic.sensors.bash"></a>

### *pydantic model* airflow_pydantic.sensors.bash.BashSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`BashSensorArgs`](#airflow_pydantic.sensors.bash.BashSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.BashSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.bash.BashSensorArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* bash_command *: str | list[str] | [BashCommands](#airflow_pydantic.utils.bash.BashCommands)* *= None*

bash command string, list of strings, or model

#### *field* env *: dict[str, str] | None* *= None*

#### *field* output_encoding *: str | None* *= None*

Output encoding for the command, default is ‘utf-8’

#### *field* retry_exit_code *: bool | None* *= None*

<a id="module-airflow_pydantic.sensors.datetime"></a>

### *pydantic model* airflow_pydantic.sensors.datetime.DateTimeSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`DateTimeSensorArgs`](#airflow_pydantic.sensors.datetime.DateTimeSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.DateTimeSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.datetime.DateTimeSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* target_time *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)]* *[Required]*

The target date and time to wait for

### *pydantic model* airflow_pydantic.sensors.datetime.DateTimeSensorAsync

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`DateTimeSensorAsyncArgs`](#airflow_pydantic.sensors.datetime.DateTimeSensorAsyncArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.DateTimeSensorAsync'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.datetime.DateTimeSensorAsyncArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* target_time *: Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)]* *[Required]*

The target date and time to wait for

#### *field* start_from_trigger *: bool | None* *= None*

If True, the sensor will start from the trigger state when used in deferrable mode

#### *field* trigger_kwargs *: dict[str, Any] | None* *= None*

Additional keyword arguments to pass to the trigger when in deferrable mode

#### *field* end_from_trigger *: bool | None* *= None*

If True, the sensor will end from the trigger state when used in deferrable mode

<a id="module-airflow_pydantic.sensors.external_task"></a>

### *pydantic model* airflow_pydantic.sensors.external_task.ExternalTaskSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`ExternalTaskSensorArgs`](#airflow_pydantic.sensors.external_task.ExternalTaskSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.ExternalTaskSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.external_task.ExternalTaskSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* external_dag_id *: str | None* *= None*

The dag_id of the external DAG to monitor

#### *field* external_task_id *: str | None* *= None*

The task_id of the external task to monitor

#### *field* external_task_ids *: list[str] | None* *= None*

A list of task_ids of the external tasks to monitor

#### *field* external_task_group_id *: str | None* *= None*

The task group ID of the external tasks to monitor

#### *field* allowed_states *: list[str] | None* *= None*

A list of allowed states for the external task(s) to be considered successful

#### *field* skipped_states *: list[str] | None* *= None*

A list of states for the external task(s) to be considered skipped

#### *field* failed_states *: list[str] | None* *= None*

A list of states for the external task(s) to be considered failed

#### *field* execution_delta *: timedelta | None* *= None*

A timedelta to add to the current task’s execution date to determine the external task’s execution date

#### *field* execution_date_fn *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | None* *= None*

A callable that takes the current task’s execution date and returns the external task’s execution date

#### *field* check_existence *: bool | None* *= None*

If True, the sensor will check for the existence of the external DAG and task(s) before monitoring

#### *field* poll_interval *: timedelta | float | None* *= None*

Time in seconds or timedelta to wait between each poke to check the external task’s state

#### *field* deferrable *: bool | None* *= None*

Set to True to enable deferrable mode for this operator

<a id="module-airflow_pydantic.sensors.filesystem"></a>

### *pydantic model* airflow_pydantic.sensors.filesystem.FileSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`FileSensorArgs`](#airflow_pydantic.sensors.filesystem.FileSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.FileSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.filesystem.FileSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* fs_conn_id *: str | None* *= None*

The connection ID to use when connecting to the filesystem

#### *field* filepath *: str | None* *= None*

The file path to check for existence

#### *field* recursive *: bool | None* *= None*

Whether to check for the file recursively in subdirectories

#### *field* deferrable *: bool | None* *= None*

Set to True to enable deferrable mode for this operator

#### *field* start_from_trigger *: bool | None* *= None*

If True, the sensor will start from the trigger state when used in deferrable mode

#### *field* trigger_kwargs *: dict[str, Any] | None* *= None*

Additional keyword arguments to pass to the trigger when in deferrable mode

<a id="module-airflow_pydantic.sensors.python"></a>

### *pydantic model* airflow_pydantic.sensors.python.PythonSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`PythonSensorArgs`](#airflow_pydantic.sensors.python.PythonSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.PythonSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.python.PythonSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* python_callable *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= None*

python_callable

#### *field* op_args *: list[object] | None* *= None*

a list of positional arguments that will get unpacked when calling your callable

#### *field* op_kwargs *: dict[str, object] | None* *= None*

a dictionary of keyword arguments that will get unpacked in your function

#### *field* templates_dict *: dict[str, object] | None* *= None*

a dictionary where the values are templates that will get templated by the Airflow engine sometime between \_\_init_\_ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)

<a id="module-airflow_pydantic.sensors.time"></a>

### *pydantic model* airflow_pydantic.sensors.time.TimeSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`TimeSensorArgs`](#airflow_pydantic.sensors.time.TimeSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.TimeSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.time.TimeSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* target_time *: time* *[Required]*

The target date and time to wait for

#### *field* deferrable *: bool | None* *= None*

If True, the sensor will operate in deferrable mode

<a id="module-airflow_pydantic.sensors.timedelta"></a>

### *pydantic model* airflow_pydantic.sensors.timedelta.TimeDeltaSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`TimeDeltaSensorArgs`](#airflow_pydantic.sensors.timedelta.TimeDeltaSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.TimeDeltaSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.timedelta.TimeDeltaSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* delta *: timedelta* *[Required]*

Time to wait before succeeding.

#### *field* deferrable *: bool | None* *= None*

If True, the sensor will operate in deferrable mode

### *pydantic model* airflow_pydantic.sensors.timedelta.WaitSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`WaitSensorArgs`](#airflow_pydantic.sensors.timedelta.WaitSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.WaitSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.timedelta.WaitSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* time_to_wait *: timedelta | int* *[Required]*

Time length to wait after the task starts before succeeding.

#### *field* deferrable *: bool | None* *= None*

If True, the sensor will operate in deferrable mode

<a id="module-airflow_pydantic.sensors.weekday"></a>

### *pydantic model* airflow_pydantic.sensors.weekday.DayOfWeekSensor

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`DayOfWeekSensorArgs`](#airflow_pydantic.sensors.weekday.DayOfWeekSensorArgs)

#### *field* operator *: Annotated[type, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.airflow.DayOfWeekSensor'*

airflow sensor path

### *pydantic model* airflow_pydantic.sensors.weekday.DayOfWeekSensorArgs

Bases: [`BaseSensorArgs`](#airflow_pydantic.sensors.base.BaseSensorArgs)

#### *field* week_day *: list[str]* *[Required]*

Day of the week to check (full name). Optionally, a set of days can also be provided using a set

#### *field* use_task_logical_date *: bool | None* *= None*

If True, uses task’s logical date to compare with week_day. Execution Date is Useful for backfilling. If False, uses system’s day of the week. Useful when you don’t want to run anything on weekdays on the system.

## Scheduling and runtime utilities

<a id="module-airflow_pydantic.utils.timetables"></a>

### *pydantic model* airflow_pydantic.utils.timetables.CronDataIntervalTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* cron *: str* *[Required]*

#### *field* timezone *: str | TimeZoneName | timedelta | None* *= None*

#### instance() → CronDataIntervalTimetable

### *pydantic model* airflow_pydantic.utils.timetables.CronTriggerTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* cron *: str* *[Required]*

#### *field* timezone *: str | TimeZoneName | timedelta | None* *= None*

#### *field* interval *: timedelta | Annotated[relativedelta, RelativeDeltaType] | None* *= None*

#### *field* run_immediately *: bool | timedelta | None* *= None*

#### instance() → CronTriggerTimetable

### *pydantic model* airflow_pydantic.utils.timetables.DeltaDataIntervalTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* delta *: timedelta | Annotated[relativedelta, RelativeDeltaType]* *[Required]*

#### instance() → DeltaDataIntervalTimetable

### *pydantic model* airflow_pydantic.utils.timetables.DeltaTriggerTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* delta *: timedelta | Annotated[relativedelta, RelativeDeltaType]* *[Required]*

#### *field* interval *: timedelta | Annotated[relativedelta, RelativeDeltaType] | None* *= None*

#### instance() → DeltaTriggerTimetable

### *pydantic model* airflow_pydantic.utils.timetables.EventsTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* event_dates *: list[Annotated[datetime | tuple[datetime, str], AfterValidator(func=\_datetime_or_datetime_and_timezone)]]* *[Required]*

#### *field* restrict_to_events *: bool | None* *= False*

#### *field* presorted *: bool | None* *= False*

#### *field* description *: str | None* *= None*

#### instance() → EventsTimetable

### airflow_pydantic.utils.timetables.FixedTimezone

alias of `timedelta`

### *pydantic model* airflow_pydantic.utils.timetables.MultipleCronTriggerTimetable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* crons *: list[str]* *[Required]*

#### *field* timezone *: str | TimeZoneName | timedelta* *[Required]*

#### *field* interval *: timedelta | Annotated[relativedelta, RelativeDeltaType] | None* *= None*

#### *field* run_immediately *: bool | timedelta | None* *= None*

#### instance() → MultipleCronTriggerTimetable

### airflow_pydantic.utils.timetables.Timezone

alias of `TimeZoneName`

<a id="module-airflow_pydantic.utils.param"></a>

### *class* airflow_pydantic.utils.param.ParamType

Bases: `object`

#### description *: str | None* *= FieldInfo(annotation=NoneType, required=False, default=None, description='Param description')*

#### title *: str | None* *= FieldInfo(annotation=NoneType, required=False, default=None, description='Param title')*

#### type *: Literal['string', 'number', 'integer', 'boolean', 'array', 'object', 'null'] | None* *= FieldInfo(annotation=NoneType, required=False, default=None, description="Param type, e.g. 'string', 'integer', 'boolean', etc.")*

#### value *: Any | None* *= FieldInfo(annotation=NoneType, required=False, default=None, description='Param value, can be any type')*

<a id="module-airflow_pydantic.utils.pool"></a>

### *pydantic model* airflow_pydantic.utils.pool.Pool

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* pool *: str* *[Required]*

Pool name

#### *field* slots *: int | None* *= None*

Number of slots in the pool

#### *field* description *: str | None* *= ''*

Pool description

#### *field* include_deferred *: bool | None* *= False*

Whether to include deferred tasks in the pool

<a id="module-airflow_pydantic.utils.variable"></a>

### *pydantic model* airflow_pydantic.utils.variable.Variable

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* key *: str* *[Required]*

Variable key

#### *field* val *: str | None* *= ''* *(alias '_val')*

Variable value

#### *field* description *: str | None* *= ''*

Variable description

#### *field* is_encrypted *: bool | None* *= False*

Whether the variable is encrypted

#### *field* deserialize_json *: bool | None* *= False*

Whether to deserialize JSON

#### get()

<a id="module-airflow_pydantic.utils.ssh_hook"></a>

<a id="module-airflow_pydantic.utils.bash"></a>

### *pydantic model* airflow_pydantic.utils.bash.BashCommands

Bases: `BaseModel`

#### *field* commands *: list[str]* *[Required]*

#### *field* quote *: str | None* *= "'"*

#### *field* escape *: bool | None* *= False*

#### *field* login *: bool | None* *= True*

#### *field* cwd *: str | None* *= ''*

#### *field* env *: dict[str, str] | None* *= None*

### airflow_pydantic.utils.bash.in_bash(command: str, quote: str | None = "'", escape: bool | None = False, login: bool | None = True, cwd: str | None = None, env: dict[str, str] | None = None) → str

Run command inside bash.

* **Parameters:**
  * **command** (*str*) – string command to run
  * **quote** (*str* *,* *optional*) – Optional simple quoting, without escaping. May cause mismatched quote problems. Defaults to “’”.
  * **escape** (*bool* *,* *optional*) – Full shell escaping. Defaults to False.
  * **login** (*bool* *,* *optional*) – Run in login shell (-l). Defaults to True.
  * **cwd** (*str* *,* *optional*) – Working directory to run the command in. Defaults to None.
* **Returns:**
  String command to run, starts with “bash”
* **Return type:**
  str

<a id="module-airflow_pydantic.utils.common"></a>

### *class* airflow_pydantic.utils.common.TriggerRule(value)

Bases: `str`, `Enum`

Class with task’s trigger rules.

#### ALL_DONE *= 'all_done'*

#### ALL_DONE_MIN_ONE_SUCCESS *= 'all_done_min_one_success'*

#### ALL_DONE_SETUP_SUCCESS *= 'all_done_setup_success'*

#### ALL_FAILED *= 'all_failed'*

#### ALL_SKIPPED *= 'all_skipped'*

#### ALL_SUCCESS *= 'all_success'*

#### ALWAYS *= 'always'*

#### NONE_FAILED *= 'none_failed'*

#### NONE_FAILED_MIN_ONE_SUCCESS *= 'none_failed_min_one_success'*

#### NONE_SKIPPED *= 'none_skipped'*

#### ONE_DONE *= 'one_done'*

#### ONE_FAILED *= 'one_failed'*

#### ONE_SUCCESS *= 'one_success'*

#### *classmethod* all_triggers() → set[str]

Return all trigger rules.

#### *classmethod* is_valid(trigger_rule: str) → bool

Validate a trigger rule.

<a id="module-airflow_pydantic.utils.env"></a>

### airflow_pydantic.utils.env.in_bash(command: str, quote: str | None = "'", escape: bool | None = False, login: bool | None = True, cwd: str | None = None, env: dict[str, str] | None = None) → str

Run command inside bash.

* **Parameters:**
  * **command** (*str*) – string command to run
  * **quote** (*str* *,* *optional*) – Optional simple quoting, without escaping. May cause mismatched quote problems. Defaults to “’”.
  * **escape** (*bool* *,* *optional*) – Full shell escaping. Defaults to False.
  * **login** (*bool* *,* *optional*) – Run in login shell (-l). Defaults to True.
  * **cwd** (*str* *,* *optional*) – Working directory to run the command in. Defaults to None.
* **Returns:**
  String command to run, starts with “bash”
* **Return type:**
  str

### airflow_pydantic.utils.env.in_conda(env: str, command: str, tool: str = 'micromamba') → str

### airflow_pydantic.utils.env.in_virtualenv(env: str, command: str) → str

<a id="module-airflow_pydantic.utils.path"></a>

### airflow_pydantic.utils.path.link(source, target, unlink: bool = True)

Link a file or directory to another location.

* **Parameters:**
  * **source** (*str*) – Source file or directory.
  * **target** (*str*) – Target file or directory.
  * **unlink** (*bool* *,* *optional*) – Unlink the target if it exists. Defaults to True.
* **Returns:**
  Command to run.
* **Return type:**
  str

<a id="module-airflow_pydantic.utils.relativedelta"></a>

## Host and balancer models

### airflow_pydantic.utils.host.ping(host, , local=True) → Callable

<a id="module-airflow_pydantic.extras.balancer.host"></a>

### *pydantic model* airflow_pydantic.extras.balancer.host.Host

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* name *: str* *[Required]*

#### *field* username *: str | None* *= None*

#### *field* password *: str | [Variable](#airflow_pydantic.utils.variable.Variable) | None* *= None*

#### *field* key_file *: str | None* *= None*

#### *field* os *: str | None* *= None*

#### *field* pool *: [Pool](#airflow_pydantic.utils.pool.Pool) | None* *= None*

#### *field* size *: int | None* *= None*

#### *field* queues *: list[str]* *[Optional]*

#### *field* tags *: list[str]* *[Optional]*

#### override(\*\*kwargs) → [Host](#airflow_pydantic.extras.balancer.host.Host)

#### hook(username: str | None = None, use_local: bool = True, \*\*hook_kwargs) → SSHHook

<a id="module-airflow_pydantic.extras.balancer.port"></a>

### *pydantic model* airflow_pydantic.extras.balancer.port.Port

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* name *: str* *= ''*

#### *field* host *: [Host](#airflow_pydantic.extras.balancer.host.Host) | None* *= None*

#### *field* host_name *: str* *= ''*

#### *field* port *: int* *= None*

Port number

#### *field* tags *: list[str]* *[Optional]*

#### *property* pool

<a id="module-airflow_pydantic.extras.balancer.query"></a>

### *pydantic model* airflow_pydantic.extras.balancer.query.BalancerHostQueryConfiguration

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* kind *: Literal['filter', 'select']* *= 'select'*

Kind of query to perform, either ‘filter’ to return a list of matching hosts or ‘select’ to return a single host.

#### *field* balancer *: BalancerConfiguration* *[Required]*

#### *field* name *: str | list[str] | None* *= None*

#### *field* queue *: str | list[str] | None* *= None*

#### *field* os *: str | list[str] | None* *= None*

#### *field* tag *: str | list[str] | None* *= None*

#### *field* custom *: Callable | Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | None* *= None*

#### execute() → list[[Host](#airflow_pydantic.extras.balancer.host.Host)] | [Host](#airflow_pydantic.extras.balancer.host.Host)

Execute the query against the provided hosts and ports.

### *pydantic model* airflow_pydantic.extras.balancer.query.BalancerPortQueryConfiguration

Bases: [`BaseModel`](#airflow_pydantic.core.base.BaseModel)

#### *field* kind *: Literal['filter', 'select']* *= 'select'*

Kind of query to perform, either ‘filter’ to return a list of matching hosts or ‘select’ to return a single host.

#### *field* balancer *: BalancerConfiguration* *[Required]*

#### *field* name *: str | list[str] | None* *= None*

#### *field* tag *: str | list[str] | None* *= None*

#### *field* custom *: Callable | Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)] | None* *= None*

#### execute() → list[[Host](#airflow_pydantic.extras.balancer.host.Host)] | [Host](#airflow_pydantic.extras.balancer.host.Host)

Execute the query against the provided hosts and ports.

### airflow_pydantic.extras.balancer.query.HostQuery

alias of [`BalancerHostQueryConfiguration`](#airflow_pydantic.extras.balancer.query.BalancerHostQueryConfiguration)

### airflow_pydantic.extras.balancer.query.PortQuery

alias of [`BalancerPortQueryConfiguration`](#airflow_pydantic.extras.balancer.query.BalancerPortQueryConfiguration)

## Common extensions

### airflow_pydantic.extras.common.airflow_functions.clean_dag_runs(session, delete_successful, delete_failed, mark_failed_as_successful, max_dagruns, days_to_keep)

### airflow_pydantic.extras.common.airflow_functions.clean_dags(session, \*\*context)

### airflow_pydantic.extras.common.airflow_functions.fail()

### airflow_pydantic.extras.common.airflow_functions.pass_()

### airflow_pydantic.extras.common.airflow_functions.skip()

<a id="module-airflow_pydantic.extras.common.clean"></a>

### *class* airflow_pydantic.extras.common.clean.DagClean(\*\*kwargs)

Bases: `PythonOperator`

### airflow_pydantic.extras.common.clean.DagCleanOperator

alias of [`DagCleanTask`](#airflow_pydantic.extras.common.clean.DagCleanTask)

### airflow_pydantic.extras.common.clean.DagCleanOperatorArgs

alias of [`DagCleanTaskArgs`](#airflow_pydantic.extras.common.clean.DagCleanTaskArgs)

### *pydantic model* airflow_pydantic.extras.common.clean.DagCleanTask

Bases: [`Task`](#airflow_pydantic.core.task.Task), [`DagCleanTaskArgs`](#airflow_pydantic.extras.common.clean.DagCleanTaskArgs)

#### *field* operator *: Annotated[object, BeforeValidator(func=get_import_path, json_schema_input_type=PydanticUndefined), PlainSerializer(func=serialize_path_as_string, return_type=str, when_used=json)]* *= 'airflow_pydantic.extras.common.clean.DagClean'*

### *pydantic model* airflow_pydantic.extras.common.clean.DagCleanTaskArgs

Bases: [`TaskArgs`](#airflow_pydantic.core.task.TaskArgs)

#### *field* delete_successful *: bool | None* *= True*

#### *field* delete_failed *: bool | None* *= True*

#### *field* mark_failed_as_successful *: bool | None* *= False*

#### *field* max_dagruns *: int | None* *= 10*

#### *field* days_to_keep *: int | None* *= 10*

### *class* airflow_pydantic.extras.common.clean.DagRunClean(\*\*kwargs)

Bases: `PythonOperator`

<a id="module-airflow_pydantic.extras.common.operators"></a>

### airflow_pydantic.extras.common.operators.FailOperator

alias of `FailTask`

### airflow_pydantic.extras.common.operators.FailOperatorArgs

alias of `FailTaskArgs`

### airflow_pydantic.extras.common.operators.PassOperator

alias of `PassTask`

### airflow_pydantic.extras.common.operators.PassOperatorArgs

alias of `PassTaskArgs`

### airflow_pydantic.extras.common.operators.SkipOperator

alias of `SkipTask`

### airflow_pydantic.extras.common.operators.SkipOperatorArgs

alias of `SkipTaskArgs`
