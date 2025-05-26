import ast
from types import NoneType
from typing import Dict, Union

from ..utils import CallablePath, ImportPath, SSHHook, serialize_path_as_string
from .utils import RenderedCode

__all__ = ("TaskRenderMixin",)


class TaskRenderMixin:
    def render(self, raw: bool = False, dag_from_context: bool = False, **kwargs: Dict[str, str]) -> RenderedCode:
        if not self.task_id:
            raise ValueError("task_id must be set to render a task")

        # Extract the importable from the operator path
        operator_import, operator_name = serialize_path_as_string(self.operator).rsplit(".", 1)
        imports = [ast.ImportFrom(module=operator_import, names=[ast.alias(name=operator_name)], level=0)]
        globals_ = []

        args = {**self.model_dump(exclude_none=True, exclude=["type_", "operator", "dependencies"]), **kwargs}
        for k, v in self.__class__.model_fields.items():
            if v.annotation in (ImportPath, CallablePath, Union[ImportPath, NoneType], Union[CallablePath, NoneType]) and k in args:
                # If the field is an ImportPath or CallablePath, we need to serialize it as a string and add it to the imports
                import_, name = serialize_path_as_string(args[k]).rsplit(".", 1)
                imports.append(ast.ImportFrom(module=import_, names=[ast.alias(name=name)], level=0))

                # Now swap the value in the args with the name
                args[k] = ast.Name(id=name, ctx=ast.Load())
            elif v.annotation in (SSHHook, Union[SSHHook, NoneType]) and k in args:
                # Add SSHHook to imports
                import_, name = serialize_path_as_string(args[k]).rsplit(".", 1)
                imports.append(ast.ImportFrom(module=import_, names=[ast.alias(name=name)], level=0))

                # Add SSHHook builder to args
                call = ast.Call(
                    func=ast.Name(id=name, ctx=ast.Load()),
                    args=[],
                    keywords=[],
                )
                for arg_name in SSHHook.__metadata__[0].__annotations__:
                    arg_value = getattr(args[k], arg_name, None)
                    if arg_value is None:
                        continue
                    if isinstance(arg_value, (str, int, float, bool)):
                        # If the value is a primitive type, we can use ast.Constant
                        # NOTE: all types in SSHHook are primitives
                        call.keywords.append(ast.keyword(arg=arg_name, value=ast.Constant(value=arg_value)))
                    else:
                        raise TypeError(f"Unsupported type for SSHHook argument '{arg_name}': {type(arg_value)}")
                args[k] = call

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
