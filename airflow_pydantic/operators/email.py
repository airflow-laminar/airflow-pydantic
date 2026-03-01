from logging import getLogger

from pydantic import Field, field_validator

from ..core import Task, TaskArgs
from ..utils import ImportPath

__all__ = (
    "EmailOperator",
    "EmailOperatorArgs",
    "EmailTask",
    "EmailTaskArgs",
)

_log = getLogger(__name__)


class EmailTaskArgs(TaskArgs):
    # email operator args
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/email/index.html
    to: str | list[str] = Field(description="List of emails to send the email to. (templated)")
    subject: str = Field(description="Subject line for the email. (templated)")
    html_content: str = Field(description="Content of the email, html markup is allowed. (templated)")
    files: list[str] | None = Field(default=None, description="File names to attach in email. (templated)")
    cc: str | list[str] | None = Field(default=None, description="List of recipients to be added in CC field. (templated)")
    bcc: str | list[str] | None = Field(default=None, description="List of recipients to be added in BCC field. (templated)")
    mime_subtype: str | None = Field(default=None, description="MIME sub content type, default is 'mixed'")
    mime_charset: str | None = Field(default=None, description="Character set parameter added to the Content-Type header, default is 'us-ascii'")
    conn_id: str | None = Field(default=None, description="The connection to use for sending the email.")
    custom_headers: dict[str, str] | None = Field(default=None, description="Additional headers to be added to the MIME message.")

    @field_validator("to", "cc", "bcc")
    @classmethod
    def validate_email_list(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            return v
        if isinstance(v, list) and all(isinstance(item, str) for item in v):
            return v
        raise ValueError("Must be a string or list of strings")


# Alias
EmailOperatorArgs = EmailTaskArgs


class EmailTask(Task, EmailTaskArgs):
    operator: ImportPath = Field(default="airflow_pydantic.airflow.EmailOperator", description="airflow operator path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        from airflow_pydantic.airflow import EmailOperator as AirflowEmailOperator, _AirflowPydanticMarker

        if not isinstance(v, type):
            raise TypeError(f"operator must be 'airflow.providers.smtp.operators.smtp.EmailOperator', got: {v}")
        if issubclass(v, _AirflowPydanticMarker):
            _log.info("EmailOperator is a marker class, returning as is")
            return v
        if not issubclass(v, AirflowEmailOperator):
            raise TypeError(f"operator must be 'airflow.providers.smtp.operators.smtp.EmailOperator', got: {v}")
        return v


# Alias
EmailOperator = EmailTask
