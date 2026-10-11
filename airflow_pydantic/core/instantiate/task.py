from typing import TYPE_CHECKING

from ...airflow import Pool as AirflowPool
from ...utils import Pool

if TYPE_CHECKING:
    from ..task import Task

__all__ = ("TaskInstantiateMixin",)


class TaskInstantiateMixin:
    def instantiate(self: "Task", **kwargs):
        if not self.task_id:
            raise ValueError("task_id must be set to instantiate a task")
        args = {**self.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]), **kwargs}

        # Handle Conversions
        if "pool" in args:
            if isinstance(args["pool"], dict):
                # Was converted by model_dump, grab
                args["pool"] = self.pool
            if isinstance(args["pool"], (AirflowPool, Pool)):
                # Convert
                args["pool"] = args["pool"].pool
            elif isinstance(args["pool"], str):
                # Leave
                pass
            else:
                raise ValueError(f"Invalid pool type: {type(args['pool'])}")
        return self.operator(**args)
