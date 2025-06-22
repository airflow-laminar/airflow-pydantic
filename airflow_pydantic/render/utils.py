import ast
from datetime import datetime, timedelta
from pathlib import Path
from types import FunctionType, MethodType
from typing import List, Tuple

from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from pkn.pydantic import serialize_path_as_string

from ..utils import SSHHook

__all__ = ("RenderedCode",)

Imports = List[str]
Globals = List[str]
TaskCode = str

RenderedCode = Tuple[Imports, Globals, TaskCode]

_LAMBDA_TYPE = type(lambda: 0)


def _islambda(v):
    return isinstance(v, _LAMBDA_TYPE) and v.__name__ == "<lambda>"


def _get_parts_from_value(key, value):
    imports = []

    if _islambda(value):
        raise NotImplementedError(
            f"Got lambda for {key}:Lambda functions are not supported in the configuration. Please use a regular function instead."
        )
    if key in ("ssh_hook", "python_callable", "output_processor"):
        from airflow.providers.ssh.hooks.ssh import SSHHook as BaseSSHHook

        if isinstance(value, BaseSSHHook):
            # Add SSHHook to imports
            import_module, name = serialize_path_as_string(value).rsplit(".", 1)
            imports.append(ast.ImportFrom(module=import_module, names=[ast.alias(name=name)], level=0))

            # Add SSHHook builder to args
            call = ast.Call(
                func=ast.Name(id=name, ctx=ast.Load()),
                args=[],
                keywords=[],
            )
            for arg_name in SSHHook.__metadata__[0].__annotations__:
                default_value = getattr(SSHHook.__metadata__[0], arg_name).default
                arg_value = getattr(value, arg_name, None)
                if arg_value is None:
                    continue
                if arg_value == default_value:
                    # Matches, can skip as well
                    continue
                if isinstance(arg_value, (str, int, float, bool)):
                    # If the value is a primitive type, we can use ast.Constant
                    # NOTE: all types in SSHHook are primitives
                    call.keywords.append(ast.keyword(arg=arg_name, value=ast.Constant(value=arg_value)))
                else:
                    raise TypeError(f"Unsupported type for SSHHook argument '{arg_name}': {type(arg_value)}")
            return imports, call

        if isinstance(value, (MethodType, FunctionType)):
            # If the field is an ImportPath or CallablePath, we need to serialize it as a string and add it to the imports
            import_module, name = serialize_path_as_string(value).rsplit(".", 1)
            imports.append(ast.ImportFrom(module=import_module, names=[ast.alias(name=name)], level=0))

            # Now swap the value in the args with the name
            if key in ("ssh_hook",):
                # For python_callable and output_processor, we need to use the name directly
                return imports, ast.Call(func=ast.Name(id=name, ctx=ast.Load()), args=[], keywords=[])
            return imports, ast.Name(id=name, ctx=ast.Load())

    if isinstance(value, TriggerRule):
        # NOTE: put before the basics types below
        # If the value is a TriggerRule, we can use a string
        return imports, ast.Constant(value=value.value)

    if isinstance(value, (str, int, float, bool)):
        # If the value is a primitive type, we can use ast.Constant
        return imports, ast.Constant(value=value)
    if value is None:
        # If the value is None, we can use ast.Constant with None
        return imports, ast.Constant(value=None)
    if isinstance(value, list):
        new_values = []
        for v in value:
            new_imports, new_value = _get_parts_from_value("", v)
            if new_imports:
                # If we have imports, we need to add them to the imports list
                imports.extend(new_imports)
            new_values.append(new_value)
        return imports, ast.List(elts=new_values, ctx=ast.Load())
    if isinstance(value, dict):
        new_keys = []
        new_values = []
        for k, v in value.items():
            new_imports, new_value = _get_parts_from_value(k, v)
            if new_imports:
                # If we have imports, we need to add them to the imports list

                imports.extend(new_imports)
            new_keys.append(ast.Constant(value=k))
            new_values.append(new_value)
        # If the value is a dict, we can use ast.Dict
        return imports, ast.Dict(
            keys=new_keys,
            values=new_values,
        )
    if isinstance(value, Path):
        imports.append(ast.ImportFrom(module="pathlib", names=[ast.alias(name="Path")], level=0))
        return imports, ast.Call(
            func=ast.Name(id="Path", ctx=ast.Load()),
            args=[ast.Constant(value=str(value))],
            keywords=[],
        )
    if isinstance(value, datetime):
        # If the value is a datetime, we can use datetime.fromisoformat
        # and convert it to a string representation
        imports.append(ast.ImportFrom(module="datetime", names=[ast.alias(name="datetime")], level=0))

        return imports, ast.Call(
            func=ast.Attribute(value=ast.Name(id="datetime", ctx=ast.Load()), attr="fromisoformat", ctx=ast.Load()),
            args=[ast.Constant(value=value.isoformat())],
            keywords=[],
        )
    if isinstance(value, timedelta):
        # If the value is a timedelta, we can use timedelta
        imports.append(ast.ImportFrom(module="datetime", names=[ast.alias(name="timedelta")], level=0))

        return imports, ast.Call(
            func=ast.Name(id="timedelta", ctx=ast.Load()),
            args=[ast.Constant(value=value.total_seconds())],
            keywords=[],
        )
    if isinstance(value, Param):
        # If the value is a Param, we can use a dict with the properties
        imports.append(ast.ImportFrom(module="airflow.models.param", names=[ast.alias(name="Param")], level=0))

        # pull out the description
        value = value.serialize()
        keywords = [
            ast.keyword(arg="description", value=ast.Constant(value=value["description"])),
        ]

        # Grab the default value from the schema if it exists
        default_value = value["schema"].pop("value", None)

        # Process title
        if "title" in value["schema"]:
            keywords.insert(0, ast.keyword(arg="title", value=ast.Constant(value=value["schema"]["title"])))

        # Process type
        if default_value is not None:
            # We can remove the "null" from the type if it exists
            if "null" in value["schema"]["type"]:
                value["schema"]["type"].remove("null")
        if isinstance(value["schema"]["type"], list) and len(value["schema"]["type"]) == 1:
            # If the type is a single item list, we can use it directly
            value["schema"]["type"] = value["schema"]["type"][0]
        new_imports, new_type = _get_parts_from_value(key, value["schema"]["type"])
        keywords.append(ast.keyword(arg="type", value=new_type))
        if new_imports:
            # If we have imports, we need to add them to the imports list
            imports.extend(new_imports)

        new_imports, new_value = _get_parts_from_value(key, value["value"])
        if new_imports:
            # If we have imports, we need to add them to the imports list
            imports.extend(new_imports)

        if new_value.value is None:
            new_value = _get_parts_from_value(key, default_value)[1]

        return imports, ast.Call(
            func=ast.Name(id="Param", ctx=ast.Load()),
            args=[new_value],
            keywords=keywords,
        )

    raise TypeError(f"Unsupported type for key: {key}, value: {type(value)}")
