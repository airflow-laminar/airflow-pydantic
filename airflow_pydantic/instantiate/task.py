__all__ = ("TaskInstantiateMixin",)


class TaskInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.task_id:
            raise ValueError("task_id must be set to instantiate a task")
        args = {**self.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]), **kwargs}
        return self.operator(**args)
