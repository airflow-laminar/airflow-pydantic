from logging import getLogger
from typing import Any, Dict, List, Literal, Optional, Type, Union

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import CallablePath, ImportPath

__all__ = (
    "PythonOperatorArgs",
    "PythonTaskArgs",
    "BranchPythonOperatorArgs",
    "BranchPythonTaskArgs",
    "ShortCircuitOperatorArgs",
    "ShortCircuitTaskArgs",
    "PythonVirtualenvOperatorArgs",
    "PythonVirtualenvTaskArgs",
    "BranchPythonVirtualenvOperatorArgs",
    "BranchPythonVirtualenvTaskArgs",
    "ExternalPythonOperatorArgs",
    "ExternalPythonTaskArgs",
    "BranchExternalPythonOperatorArgs",
    "BranchExternalPythonTaskArgs",
    "PythonOperator",
    "PythonTask",
    "BranchPythonOperator",
    "BranchPythonTask",
    "ShortCircuitOperator",
    "ShortCircuitTask",
    "PythonVirtualenvOperator",
    "PythonVirtualenvTask",
    "BranchPythonVirtualenvOperator",
    "BranchPythonVirtualenvTask",
    "ExternalPythonOperator",
    "ExternalPythonTask",
    "BranchExternalPythonOperator",
    "BranchExternalPythonTask",
)

_log = getLogger(__name__)


class PythonTaskArgs(TaskArgs):
    # python operator args
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonOperator
    python_callable: CallablePath = Field(default=None, description="python_callable")
    op_args: Optional[List[object]] = Field(
        default=None, description="a list of positional arguments that will get unpacked when calling your callable"
    )
    op_kwargs: Optional[Dict[str, object]] = Field(
        default=None, description="a dictionary of keyword arguments that will get unpacked in your function"
    )
    templates_dict: Optional[Dict[str, object]] = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)",
    )
    templates_exts: Optional[List[str]] = Field(
        default=None, description="a list of file extensions to resolve while processing templated fields, for examples ['.sql', '.hql']"
    )
    show_return_value_in_logs: Optional[bool] = Field(
        default=None,
        description="a bool value whether to show return_value logs. Defaults to True, which allows return value log output. It can be set to False",
    )


PythonOperatorArgs = PythonTaskArgs


class BranchPythonTaskArgs(PythonTaskArgs): ...


# Alias
BranchPythonOperatorArgs = BranchPythonTaskArgs


class ShortCircuitTaskArgs(PythonTaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.ShortCircuitOperator
    ignore_downstream_trigger_rules: Optional[bool] = Field(
        default=None,
        description=" If set to True, all downstream tasks from this operator task will be skipped. This is the default behavior. If set to False, the direct, downstream task(s) will be skipped but the trigger_rule defined for a other downstream tasks will be respected.",
    )


# Alias
ShortCircuitOperatorArgs = ShortCircuitTaskArgs


class PythonVirtualenvTaskArgs(PythonTaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonVirtualenvOperator
    python_callable: CallablePath = Field(
        description="A python function with no references to outside variables, defined with def, which will be run in a virtual environment."
    )
    requirements: Optional[List[str]] = Field(
        default=None, description="Either a list of requirement strings, or a (templated) “requirements file” as specified by pip."
    )
    python_version: Optional[str] = Field(
        default=None, description="The Python version to run the virtual environment with. Note that both 2 and 2.7 are acceptable forms."
    )
    serializer: Literal["pickle", "cloudpickle", "dill"] = Field(
        default="pickle", description="Which serializer use to serialize the args and result."
    )
    system_site_packages: Optional[bool] = Field(
        default=None,
        description="Whether to include system_site_packages in your virtual environment. See virtualenv documentation for more information.",
    )
    pip_install_options: Optional[List[str]] = Field(
        default=None, description="a list of pip install options when installing requirements See ‘pip install -h’ for available options"
    )
    op_args: Optional[List[Any]] = Field(default=None, description="A list of positional arguments to pass to python_callable.")
    op_kwargs: Optional[Dict[str, Any]] = Field(default=None, description="A dict of keyword arguments to pass to python_callable.")
    string_args: Optional[List[str]] = Field(
        default=None,
        description="Strings that are present in the global var virtualenv_string_args, available to python_callable at runtime as a list[str]. Note that args are split by newline.",
    )
    templates_dict: Optional[Dict[str, Any]] = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied",
    )
    templates_exts: Optional[List[str]] = Field(
        default=None, description="a list of file extensions to resolve while processing templated fields, for examples ['.sql', '.hql']"
    )
    expect_airflow: Optional[bool] = Field(
        default=None,
        description="expect Airflow to be installed in the target environment. If true, the operator will raise warning if Airflow is not installed, and it will attempt to load Airflow macros when starting.",
    )
    skip_on_exit_code: Optional[Union[int, List[int]]] = Field(
        default=None,
        description="If python_callable exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.",
    )
    index_urls: Optional[Union[List[str], str]] = Field(
        default=None,
        description="an optional list of index urls to load Python packages from. If not provided the system pip conf will be used to source packages from.",
    )
    index_urls_from_connection_ids: Optional[Union[List[str], str]] = Field(
        default=None,
        description="An optional list of PackageIndex connection IDs. Will be appended to index_urls.",
    )
    venv_cache_path: Optional[str] = Field(
        default=None,
        description="Optional path to the virtual environment parent folder in which the virtual environment will be cached, creates a sub-folder venv-{hash} whereas hash will be replaced with a checksum of requirements. If not provided the virtual environment will be created and deleted in a temp folder for every execution.",
    )
    env_vars: Optional[Dict[str, str]] = Field(
        default=None,
        description="A dictionary containing additional environment variables to set for the virtual environment when it is executed.",
    )
    inherit_env: Optional[bool] = Field(
        default=None,
        description="Whether to inherit the current environment variables when executing the virtual environment. If set to True, the virtual environment will inherit the environment variables of the parent process (os.environ). If set to False, the virtual environment will be executed with a clean environment.",
    )


# Alias
PythonVirtualenvOperatorArgs = PythonVirtualenvTaskArgs


class BranchPythonVirtualenvTaskArgs(PythonVirtualenvTaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.PythonVirtualenvOperator
    ...


# Alias
BranchPythonVirtualenvOperatorArgs = BranchPythonVirtualenvTaskArgs


class ExternalPythonTaskArgs(PythonTaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.ExternalPythonOperator
    python: str = Field(
        description="Full path string (file-system specific) that points to a Python binary inside a virtual environment that should be used (in VENV/bin folder). Should be absolute path (so usually start with “/” or “X:/” depending on the filesystem/os used)."
    )
    python_callable: CallablePath = Field(
        description="A python function with no references to outside variables, defined with def, which will be run in a virtual environment."
    )
    serializer: Optional[Literal["pickle", "cloudpickle", "dill"]] = Field(
        default=None,
        description="Which serializer use to serialize the args and result. It can be one of the following: 'pickle' (default) Use pickle for serialization. Included in the Python Standard Library.; 'cloudpickle' Use cloudpickle for serialize more complex types, this requires to include cloudpickle in your requirements.; 'dill' Use dill for serialize more complex types, this requires to include dill in your requirements.",
    )
    op_args: Optional[List[Any]] = Field(default=None, description="A list of positional arguments to pass to python_callable.")
    op_kwargs: Optional[Dict[str, Any]] = Field(default=None, description="A dict of keyword arguments to pass to python_callable.")
    string_args: Optional[List[str]] = Field(
        default=None,
        description="Strings that are present in the global var external_python_string_args, available to python_callable at runtime as a list[str]. Note that args are split by newline.",
    )
    templates_dict: Optional[Dict[str, Any]] = Field(
        default=None,
        description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied",
    )
    templates_exts: Optional[List[str]] = Field(
        default=None, description="a list of file extensions to resolve while processing templated fields, for examples ['.sql', '.hql']"
    )
    expect_airflow: Optional[bool] = Field(
        default=None,
        description="expect Airflow to be installed in the target environment. If true, the operator will raise warning if Airflow is not installed, and it will attempt to load Airflow macros when starting.",
    )
    skip_on_exit_code: Optional[Union[int, List[int]]] = Field(
        default=None,
        description="If python_callable exits with this exit code, leave the task in skipped state (default: None). If set to None, any non-zero exit code will be treated as a failure.",
    )
    env_vars: Optional[Dict[str, str]] = Field(
        default=None,
        description="A dictionary containing additional environment variables to set for the external python environment when it is executed.",
    )
    inherit_env: Optional[bool] = Field(
        default=None,
        description="Whether to inherit the current environment variables when executing the external python. If set to True, the external python will inherit the environment variables of the parent process (os.environ). If set to False, the external python will be executed with a clean environment.",
    )


# Alias
ExternalPythonOperatorArgs = ExternalPythonTaskArgs


class BranchExternalPythonTaskArgs(ExternalPythonTaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/python/index.html#airflow.providers.standard.operators.python.BranchExternalPythonOperator
    ...


# Alias
BranchExternalPythonOperatorArgs = BranchExternalPythonTaskArgs


class PythonTask(Task, PythonTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.PythonOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> ImportPath:
        from airflow_pydantic.airflow import PythonOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.PythonOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("PythonOperator is a marker class, returning as is")
            return v
        if not issubclass(v, PythonOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.PythonOperator', got: {v}")
        return v


# Alias
PythonOperator = PythonTask


class BranchPythonTask(Task, BranchPythonTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.BranchPythonOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import BranchPythonOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchPythonOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BranchPythonOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BranchPythonOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchPythonOperator', got: {v}")
        return v


# Alias
BranchPythonOperator = BranchPythonTask


class ShortCircuitTask(Task, ShortCircuitTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.ShortCircuitOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import ShortCircuitOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.ShortCircuitOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("ShortCircuitOperator is a marker class, returning as is")
            return v
        if not issubclass(v, ShortCircuitOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.ShortCircuitOperator', got: {v}")
        return v


# Alias
ShortCircuitOperator = ShortCircuitTask


class PythonVirtualenvTask(Task, PythonVirtualenvTaskArgs):
    operator: ImportPath = Field(
        default="airflow_pydantic.airflow.PythonVirtualenvOperator", description="airflow operator path", validate_default=True
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import PythonVirtualenvOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.PythonVirtualenvOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("PythonVirtualenvOperator is a marker class, returning as is")
            return v
        if not issubclass(v, PythonVirtualenvOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.PythonVirtualenvOperator', got: {v}")
        return v


# Alias
PythonVirtualenvOperator = PythonVirtualenvTask


class BranchPythonVirtualenvTask(Task, BranchPythonVirtualenvTaskArgs):
    operator: ImportPath = Field(
        default="airflow_pydantic.airflow.BranchPythonVirtualenvOperator", description="airflow operator path", validate_default=True
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import BranchPythonVirtualenvOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchPythonVirtualenvOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BranchPythonVirtualenvOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BranchPythonVirtualenvOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchPythonVirtualenvOperator', got: {v}")
        return v


# Alias
BranchPythonVirtualenvOperator = BranchPythonVirtualenvTask


class ExternalPythonTask(Task, ExternalPythonTaskArgs):
    operator: ImportPath = Field(
        default="airflow_pydantic.airflow.ExternalPythonOperator", description="airflow operator path", validate_default=True
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import ExternalPythonOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.ExternalPythonOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("ExternalPythonOperator is a marker class, returning as is")
            return v
        if not issubclass(v, ExternalPythonOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.ExternalPythonOperator', got: {v}")
        return v


# Alias
ExternalPythonOperator = ExternalPythonTask


class BranchExternalPythonTask(Task, BranchExternalPythonTaskArgs):
    operator: ImportPath = Field(
        default="airflow_pydantic.airflow.BranchExternalPythonOperator", description="airflow operator path", validate_default=True
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import BranchExternalPythonOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchExternalPythonOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("BranchExternalPythonOperator is a marker class, returning as is")
            return v
        if not issubclass(v, BranchExternalPythonOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.python.BranchExternalPythonOperator', got: {v}")
        return v


# Alias
BranchExternalPythonOperator = BranchExternalPythonTask
