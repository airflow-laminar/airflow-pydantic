from logging import getLogger
from typing import Dict, List, Optional, Type

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath, Param

__all__ = (
    "HITLOperatorArgs",
    "HITLTaskArgs",
    "ApprovalOperatorArgs",
    "ApprovalTaskArgs",
    "HITLBranchOperatorArgs",
    "HITLBranchTaskArgs",
    "HITLEntryOperatorArgs",
    "HITLEntryTaskArgs",
    "HITLOperator",
    "HITLTask",
    "ApprovalOperator",
    "ApprovalTask",
    "HITLBranchOperator",
    "HITLBranchTask",
    "HITLEntryOperator",
    "HITLEntryTask",
)

_log = getLogger(__name__)


class HITLTaskArgs(TaskArgs):
    # https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html#airflow.providers.standard.operators.hitl.HITLOperator
    subject: Optional[str] = Field(default=None, description="Headline/subject presented to the user for the interaction task")
    options: Optional[List[str]] = Field(default=None, description="List of options that the an user can select from to complete the task.")
    body: Optional[str] = Field(
        default=None, description=" Descriptive text (with Markdown support) that gives the details that are needed to decide."
    )
    defaults: Optional[List[str]] = Field(default=None, description="The default options and the options that are taken if timeout is passed.")
    multiple: Optional[bool] = Field(default=None, description="Whether the user can select one or multiple options.")
    params: Optional[Dict[str, Param]] = Field(
        default=None,
        description="dictionary of parameter definitions that are in the format of Dag params such that a Form Field can be rendered. Entered data is validated (schema, required fields) like for a Dag run and added to XCom of the task result.",
    )


# Alias
HITLOperatorArgs = HITLTaskArgs


class ApprovalTaskArgs(HITLTaskArgs): ...


# Alias
ApprovalOperatorArgs = ApprovalTaskArgs


class HITLBranchTaskArgs(HITLTaskArgs): ...


# Alias
HITLBranchOperatorArgs = HITLBranchTaskArgs


class HITLEntryTaskArgs(HITLTaskArgs): ...


# Alias
HITLEntryOperatorArgs = HITLEntryTaskArgs


class HITLTask(Task, HITLOperatorArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.HITLOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import HITLOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("HITLOperator is a marker class, returning as is")
            return v
        if not issubclass(v, HITLOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLOperator', got: {v}")
        return v


# Alias
HITLOperator = HITLTask


class ApprovalTask(Task, ApprovalTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.ApprovalOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import ApprovalOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.ApprovalOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("ApprovalOperator is a marker class, returning as is")
            return v
        if not issubclass(v, ApprovalOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.ApprovalOperator', got: {v}")
        return v


# Alias
ApprovalOperator = ApprovalTask


class HITLBranchTask(Task, HITLBranchTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.HITLBranchOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import HITLBranchOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLBranchOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("HITLBranchOperator is a marker class, returning as is")
            return v
        if not issubclass(v, HITLBranchOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLBranchOperator', got: {v}")
        return v


# Alias
HITLBranchOperator = HITLBranchTask


class HITLEntryTask(Task, HITLEntryTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.HITLEntryOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        from airflow_pydantic.airflow import HITLEntryOperator, _AirflowPydanticMarker

        if not isinstance(v, Type):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLEntryOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("HITLEntryOperator is a marker class, returning as is")
            return v
        if not issubclass(v, HITLEntryOperator):
            raise ValueError(f"operator must be 'airflow.providers.standard.operators.hitl.HITLEntryOperator', got: {v}")
        return v


# Alias
HITLEntryOperator = HITLEntryTask
