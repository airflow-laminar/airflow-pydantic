import ast
from typing import Dict

from pkn.pydantic import serialize_path_as_string

from .utils import RenderedCode, _get_parts_from_value

__all__ = (
    "render_base_task_args",
    "TaskRenderMixin",
)


def render_base_task_args(self, raw: bool = False, **kwargs: Dict[str, str]) -> RenderedCode:
    # Extract the importable from the operator path
    imports = []
    globals_ = []

    args = {**self.model_dump(exclude_unset=True, exclude=["type_"]), **kwargs}
    for k, v in args.items():
        new_imports, value = _get_parts_from_value(k, v)
        if new_imports:
            imports.extend(new_imports)
        if isinstance(value, ast.AST):
            # If the value is already an AST node, we can use it directly
            args[k] = value
        else:
            # Otherwise, we need to convert it to an AST node
            args[k] = ast.Constant(value=value)

    # Return dictionary literal of args
    value = ast.Dict(
        keys=[ast.Constant(value=k) for k in args.keys()],
        values=[v if isinstance(v, ast.AST) else ast.Constant(value=v) for v in args.values()],
    )
    if not raw:
        # If not raw, we need to convert the imports and inside_dag to a string representation
        imports = [ast.unparse(i) for i in imports]
        globals_ = [ast.unparse(i) for i in globals_]
        value = ast.unparse(value)
    return (
        imports,
        globals_,
        value,
    )


class TaskRenderMixin:
    def render(self, raw: bool = False, dag_from_context: bool = False, **kwargs: Dict[str, str]) -> RenderedCode:
        if not self.task_id:
            raise ValueError("task_id must be set to render a task")

        # Extract the importable from the operator path
        operator_import, operator_name = serialize_path_as_string(self.operator).rsplit(".", 1)
        imports = [ast.ImportFrom(module=operator_import, names=[ast.alias(name=operator_name)], level=0)]
        globals_ = []

        args = {**self.model_dump(exclude_unset=True, exclude=["type_", "operator", "dependencies"]), **kwargs}
        for k, v in args.items():
            # For a specific SSH Hook, we want to replace the password with a variable invocation
            if (
                k == "ssh_hook"
                and (v is None or (hasattr(self, "ssh_hook_external") and self.ssh_hook_external))
                and hasattr(self, "ssh_hook_foo")
                and self.ssh_hook_foo
            ):
                # If we have a callable, we want to import it
                foo_import, foo_name = serialize_path_as_string(self.ssh_hook_foo).rsplit(".", 1)
                imports.append(
                    ast.ImportFrom(
                        module=foo_import,
                        names=[ast.alias(name=foo_name)],
                        level=0,
                    )
                )
                # Replace the ssh_hook with the callable
                args[k] = ast.Call(func=ast.Name(id=foo_name, ctx=ast.Load()), args=[], keywords=[])
                self.ssh_hook = None  # Clear the ssh_hook to avoid confusion
                continue

            # Default case
            import_, value = _get_parts_from_value(k, v)
            if import_:
                imports.extend(import_)
            if isinstance(value, ast.AST):
                # If the value is already an AST node, we can use it directly
                args[k] = value
            else:
                # Otherwise, we need to convert it to an AST node
                args[k] = ast.Constant(value=value)

            # If it was a balancer hook using a variable, we want to handle differently
            if k == "ssh_hook" and hasattr(self, "ssh_hook_host") and self.ssh_hook_host:
                # If we have a host, and the host looks in a variable, lets
                # use that instead of printing the password.
                if self.ssh_hook_host.username and not self.ssh_hook_host.password and self.ssh_hook_host.password_variable:
                    imports.append(
                        ast.ImportFrom(
                            module="airflow.models.variable",
                            names=[ast.alias(name="Variable")],
                            level=0,
                        )
                    )

                    call: ast.Call = args[k]
                    for k in call.keywords:
                        if k.arg == "password":
                            variable_get = ast.Call(
                                func=ast.Attribute(value=ast.Name(id="Variable", ctx=ast.Load()), attr="get", ctx=ast.Load()),
                                args=[ast.Constant(value=self.ssh_hook_host.password_variable)],
                                keywords=[],
                            )
                            if self.ssh_hook_host.password_variable_key:
                                # Use bracket operator to get the key called password_variable_key
                                k.value = ast.Subscript(
                                    value=variable_get,
                                    slice=ast.Constant(value=self.ssh_hook_host.password_variable_key),
                                )
                            else:
                                k.value = variable_get

        inside_dag = ast.Call(
            func=ast.Name(id=operator_name, ctx=ast.Load()),
            args=[],
            keywords=[ast.keyword(arg=k, value=ast.Constant(value=v) if not isinstance(v, ast.AST) else v) for k, v in args.items()]
            + ([] if not dag_from_context else [ast.keyword(arg="dag", value=ast.Name(id="dag", ctx=ast.Load()))]),
        )

        if not raw:
            # If not raw, we need to convert the imports and inside_dag to a string representation
            imports = [ast.unparse(i) for i in imports]
            globals_ = [ast.unparse(i) for i in globals_]
            inside_dag = ast.unparse(inside_dag)
        return (
            imports,
            globals_,
            inside_dag,
        )
