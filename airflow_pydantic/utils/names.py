__all__ = (
    "_task_id_to_python_name",
    "_better_task_id",
)


def _task_id_to_python_name(task_id):
    # Replace underscore, space, and period with underscore

    ret = task_id.replace("-", "_").replace(" ", "_").replace(".", "_")

    if not ret.isidentifier():
        raise ValueError(f"Task ID '{ret}' is not a valid Python identifier")

    return ret


def _better_task_id(task_id):
    # Replace underscore, space, and period with hyphen
    return task_id.replace("_", "-").replace(" ", "-").replace(".", "-")
