import ast
from datetime import datetime, time, timedelta
from pathlib import Path
from types import FunctionType, MethodType
from typing import List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from pendulum import DateTime, Timezone
from pkn.pydantic import serialize_path_as_string
from pydantic import BaseModel

from ...airflow import Param as AirflowParam, Pool as AirflowPool
from ...utils import (
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
    EventsTimetable,
    MultipleCronTriggerTimetable,
    Pool,
    SSHHook,
    TriggerRule,
    Variable,
)

__all__ = ("RenderedCode",)

Imports = List[str]
Globals = List[str]
TaskCode = str

RenderedCode = Tuple[Imports, Globals, TaskCode]

_LAMBDA_TYPE = type(lambda: 0)


class RenderError(TypeError): ...


def _islambda(v):
    return isinstance(v, _LAMBDA_TYPE) and v.__name__ == "<lambda>"


def _build_pool_callable(pool) -> Tuple[ast.ImportFrom, ast.Call]:
    imports = []
    if isinstance(pool, Pool):
        # Swap
        pool = pool.model_dump(exclude_unset=True)
    elif isinstance(pool, AirflowPool):
        pool = {
            "pool": pool.pool,
            "slots": pool.slots,
            "description": pool.description if pool.description is not None else "",
            "include_deferred": pool.include_deferred if pool.include_deferred is not None else False,
        }
    elif isinstance(pool, str):
        if pool == "default":
            # If default, leave alone
            return [], ast.Constant(value=pool)
        pool = {"pool": pool}
    elif isinstance(pool, dict):
        # Leave it
        pass
    else:
        raise TypeError(f"Unsupported type for pool: {type(pool)}. Expected Pool, AirflowPool, or str.")

    if len(pool.keys()) == 1:
        imports.append(
            ast.ImportFrom(
                module="airflow.models.pool",
                names=[ast.alias(name="Pool")],
                level=0,
            )
        )
        # Replace the pool with the Pool class
        return imports, ast.Attribute(
            value=ast.Call(
                func=ast.Attribute(value=ast.Name(id="Pool", ctx=ast.Load()), attr="get_pool", ctx=ast.Load()),
                args=[ast.Constant(value=pool["pool"])],
                keywords=[],
            ),
            attr="pool",
            ctx=ast.Load(),
        )
    imports.append(
        ast.ImportFrom(
            module="airflow.models.pool",
            names=[ast.alias(name="Pool")],
            level=0,
        )
    )
    # Replace the pool with the Pool class
    return imports, ast.Attribute(
        value=ast.Call(
            func=ast.Attribute(value=ast.Name(id="Pool", ctx=ast.Load()), attr="create_or_update_pool", ctx=ast.Load()),
            args=[],
            keywords=[
                ast.keyword(arg="name", value=ast.Constant(value=pool["pool"])),
                ast.keyword(arg="slots", value=ast.Constant(value=pool.get("slots", 128))),
                ast.keyword(arg="description", value=ast.Constant(value=pool.get("description", ""))),
                ast.keyword(arg="include_deferred", value=ast.Constant(value=pool.get("include_deferred", False))),
            ],
        ),
        attr="pool",
        ctx=ast.Load(),
    )


def _build_param_callable(param, key) -> Tuple[List[ast.ImportFrom], ast.Call]:
    imports = []
    # If the value is a Param, we can use a dict with the properties
    imports.append(ast.ImportFrom(module="airflow.models.param", names=[ast.alias(name="Param")], level=0))

    # pull out the description
    param = param.serialize()
    keywords = [
        ast.keyword(arg="description", value=ast.Constant(value=param["description"])),
    ]

    # Grab the default value from the schema if it exists
    default_value = param["schema"].pop("value", None)

    # Process title
    if "title" in param["schema"]:
        keywords.insert(0, ast.keyword(arg="title", value=ast.Constant(value=param["schema"]["title"])))

    # Process type
    if default_value is not None:
        # We can remove the "null" from the type if it exists
        if "null" in param["schema"]["type"]:
            param["schema"]["type"].remove("null")
    if isinstance(param["schema"]["type"], list) and len(param["schema"]["type"]) == 1:
        # If the type is a single item list, we can use it directly
        param["schema"]["type"] = param["schema"]["type"][0]
    new_imports, new_type = _get_parts_from_value(key, param["schema"]["type"])
    keywords.append(ast.keyword(arg="type", value=new_type))
    if new_imports:
        # If we have imports, we need to add them to the imports list
        imports.extend(new_imports)

    new_imports, new_value = _get_parts_from_value(key, param["value"])
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


def _build_ssh_hook_callable(foo) -> Tuple[List[ast.ImportFrom], ast.Call]:
    imports = []
    # If we have a callable, we want to import it
    foo_import, foo_name = serialize_path_as_string(foo).rsplit(".", 1)
    imports.append(
        ast.ImportFrom(
            module=foo_import,
            names=[ast.alias(name=foo_name)],
            level=0,
        )
    )
    # Replace the ssh_hook with the callable
    ret = ast.Call(func=ast.Name(id=foo_name, ctx=ast.Load()), args=[], keywords=[])
    return imports, ret


def _build_ssh_hook_with_variable(host, call: ast.Call) -> Tuple[List[ast.ImportFrom], ast.Call]:
    imports = []
    if isinstance(host.password, Variable):
        has_any_variable = False

        if isinstance(call, ast.Call):
            for k in call.keywords:
                if k.arg == "password":
                    variable_get = ast.Call(
                        func=ast.Attribute(value=ast.Name(id="AirflowVariable", ctx=ast.Load()), attr="get", ctx=ast.Load()),
                        args=[ast.Constant(value=host.password.key)],
                        keywords=[],
                    )
                    if host.password.deserialize_json:
                        # Use bracket operator to get the key called "password"
                        variable_get = ast.Call(
                            func=ast.Attribute(value=ast.Name(id="AirflowVariable", ctx=ast.Load()), attr="get", ctx=ast.Load()),
                            args=[ast.Constant(value=host.password.key)],
                            keywords=[ast.keyword(arg="deserialize_json", value=ast.Constant(value=True))],
                        )
                        # TODO maybe not "password"
                        k.value = ast.Subscript(
                            value=variable_get,
                            slice=ast.Constant(value="password"),
                        )
                    else:
                        k.value = variable_get
                    has_any_variable = True
        else:
            raise NotImplementedError(f"Got unexpected call type for `{ast.unparse(call)}`: {type(call)}")

        if has_any_variable:
            imports.append(
                ast.ImportFrom(
                    module="airflow.models.variable",
                    names=[ast.alias(name="Variable", asname="AirflowVariable")],
                    level=0,
                )
            )
    return imports, call


def _get_parts_from_value(key, value, model_ref: Optional[BaseModel] = None):
    from airflow_pydantic import Host, Port

    imports = []

    # For certain types, we want to reset the recursive model_dump back
    # to allow type-specific processing
    # Reverted types:
    #   - Host
    #   - Port
    #   - Pool
    # NOTE: See below for backup
    if model_ref:
        if isinstance(
            getattr(model_ref, key),
            (
                Host,
                Port,
                Pool,
                Variable,
                CronDataIntervalTimetable,
                CronTriggerTimetable,
                MultipleCronTriggerTimetable,
                DeltaDataIntervalTimetable,
                DeltaTriggerTimetable,
                EventsTimetable,
            ),
        ):
            value = getattr(model_ref, key)
    if _islambda(value):
        raise NotImplementedError(
            f"Got lambda for {key}:Lambda functions are not supported in the configuration. Please use a regular function instead."
        )
    if key in ("ssh_hook", "python_callable", "output_processor"):
        try:
            from airflow_pydantic.airflow import SSHHook as BaseSSHHook

            if isinstance(value, BaseSSHHook):
                # Add SSHHook to imports
                import_module, name = serialize_path_as_string(value).rsplit(".", 1)
                imports.append(ast.ImportFrom(module="airflow.providers.ssh.hooks.ssh", names=[ast.alias(name="SSHHook")], level=0))

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
        except ImportError:
            # If SSHHook is not available, we can skip it
            pass

        if isinstance(value, (MethodType, FunctionType)):
            # If the field is an ImportPath or CallablePath, we need to serialize it as a string and add it to the imports
            import_module, name = serialize_path_as_string(value).rsplit(".", 1)
            imports.append(ast.ImportFrom(module=import_module, names=[ast.alias(name=name)], level=0))

            # Now swap the value in the args with the name
            if key in ("ssh_hook",):
                # For python_callable and output_processor, we need to use the name directly
                return imports, ast.Call(func=ast.Name(id=name, ctx=ast.Load()), args=[], keywords=[])
            return imports, ast.Name(id=name, ctx=ast.Load())

    if key in ("pool",):
        return _build_pool_callable(value)

    if isinstance(value, Host):
        imports.append(ast.ImportFrom(module="airflow_pydantic", names=[ast.alias(name="Host")], level=0))

        # Construct Call with host
        keywords = []
        for k, v in value.model_dump(exclude_unset=True).items():
            keyword_imports, keyword_value = _get_parts_from_value(k, v, value)
            if keyword_imports:
                imports.extend(keyword_imports)
            keywords.append(ast.keyword(arg=k, value=keyword_value))
        call = ast.Call(
            func=ast.Name(id="Host", ctx=ast.Load()),
            args=[],
            keywords=keywords,
        )
        return imports, call

    if isinstance(value, Port):
        imports.append(ast.ImportFrom(module="airflow_pydantic", names=[ast.alias(name="Port")], level=0))
        keywords = []
        for k, v in value.model_dump(exclude_unset=True).items():
            keyword_imports, keyword_value = _get_parts_from_value(k, v, value)
            if keyword_imports:
                imports.extend(keyword_imports)
            keywords.append(ast.keyword(arg=k, value=keyword_value))
        call = ast.Call(
            func=ast.Name(id="Port", ctx=ast.Load()),
            args=[],
            keywords=keywords,
        )
        return imports, call

    if isinstance(value, Pool):
        new_import, new_value = _build_pool_callable(value)
        imports.append(new_import)
        return imports, new_value

    if isinstance(value, Variable):
        imports.append(ast.ImportFrom(module="airflow_pydantic", names=[ast.alias(name="Variable")], level=0))
        keywords = []
        for k, v in value.model_dump(exclude_unset=True).items():
            keyword_imports, keyword_value = _get_parts_from_value(k, v, value)
            if keyword_imports:
                imports.extend(keyword_imports)
            keywords.append(ast.keyword(arg=k, value=keyword_value))
        call = ast.Call(
            func=ast.Name(id="Variable", ctx=ast.Load()),
            args=[],
            keywords=keywords,
        )
        return imports, call

    if isinstance(
        value,
        (
            CronDataIntervalTimetable,
            CronTriggerTimetable,
            MultipleCronTriggerTimetable,
            DeltaDataIntervalTimetable,
            DeltaTriggerTimetable,
            EventsTimetable,
        ),
    ):
        if isinstance(value, (CronDataIntervalTimetable, DeltaDataIntervalTimetable)):
            import_module = "airflow.timetables.interval"
        elif isinstance(value, (CronTriggerTimetable, DeltaTriggerTimetable, MultipleCronTriggerTimetable)):
            import_module = "airflow.timetables.trigger"
        elif isinstance(value, EventsTimetable):
            import_module = "airflow.timetables.events"
        else:
            # Hope for the best
            import_module = "airflow.timetables"
        imports.append(ast.ImportFrom(module=import_module, names=[ast.alias(name=value.__class__.__name__)], level=0))
        args = []
        keywords = []
        for k, v in value.model_dump(exclude_unset=True).items():
            if k == "crons":
                # vararg of strs
                for arg in v:
                    args.append(ast.Constant(value=arg))
            else:
                keyword_imports, keyword_value = _get_parts_from_value(k, v, value)
                if keyword_imports:
                    imports.extend(keyword_imports)
                keywords.append(ast.keyword(arg=k, value=keyword_value))
        return imports, ast.Call(
            func=ast.Name(id=value.__class__.__name__, ctx=ast.Load()),
            args=args,
            keywords=keywords,
        )

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
        # Check if its actually a model!
        # NOTE: See abvoe for primary
        if model_ref and isinstance(getattr(model_ref, key, None), BaseModel):
            try:
                return _get_parts_from_value(key, getattr(model_ref, key), model_ref)
            except RenderError:
                # Just parse as a dict, some types we don't handle specifically:
                # - SueprvisorAirflowConfiguration
                # - SupervisorSSHAirflowConfiguration
                pass
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
    if isinstance(value, DateTime):
        # NOTE: Handle pendulum datetime
        imports.append(ast.ImportFrom(module="pendulum", names=[ast.alias(name="datetime", asname="pdatetime")], level=0))
        kwargs = []
        for attr in ("year", "month", "day", "tz"):
            new_imports, new_value = _get_parts_from_value(attr, getattr(value, attr))
            imports.extend(new_imports)
            kwargs.append(ast.keyword(arg=attr, value=new_value))
        return imports, ast.Call(
            func=ast.Name(id="pdatetime", ctx=ast.Load()),
            args=[],
            keywords=kwargs,
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
    if isinstance(value, Timezone):
        imports.append(ast.ImportFrom(module="pendulum", names=[ast.alias(name="Timezone")], level=0))
        return imports, ast.Call(
            func=ast.Name(id="Timezone", ctx=ast.Load()),
            args=[ast.Constant(value=value.name)],
            keywords=[],
        )
    if isinstance(value, timedelta):
        # If the value is a timedelta, we can use timedelta
        imports.append(ast.ImportFrom(module="datetime", names=[ast.alias(name="timedelta")], level=0))

        return imports, ast.Call(
            func=ast.Name(id="timedelta", ctx=ast.Load()),
            args=[],
            keywords=[ast.keyword(arg="seconds", value=ast.Constant(value=value.total_seconds()))],
        )
    if isinstance(value, time):
        value: time
        # If the value is a time, we can use time.fromisoformat
        imports.append(ast.ImportFrom(module="datetime", names=[ast.alias(name="time")], level=0))

        return imports, ast.Call(
            func=ast.Name(id="time", ctx=ast.Load()),
            args=[
                ast.Constant(value=value.hour),
                ast.Constant(value=value.minute),
                ast.Constant(value=value.second),
                ast.Constant(value=value.microsecond),
                # TODO tzinfo
            ],
            keywords=[],
        )
    if isinstance(value, relativedelta):
        imports.append(ast.ImportFrom(module="dateutil.relativedelta", names=[ast.alias(name="relativedelta")], level=0))
        kwargs = []
        for attr in (
            "years",
            "months",
            "days",
            "leapdays",
            "hours",
            "minutes",
            "seconds",
            "microseconds",
            "year",
            "month",
            "day",
            "weekday",
            "hour",
            "minute",
            "second",
            "microsecond",
        ):
            val = getattr(value, attr)
            if val is not None:
                kwargs.append(ast.keyword(arg=attr, value=ast.Constant(value=val)))
        return imports, ast.Call(
            func=ast.Name(id="relativedelta", ctx=ast.Load()),
            args=[],
            keywords=kwargs,
        )
    if isinstance(value, AirflowParam):
        new_imports, new_value = _build_param_callable(value, key)
        imports.extend(new_imports)
        return imports, new_value

    if isinstance(value, AirflowPool):
        new_import, new_value = _build_pool_callable(value)
        imports.append(new_import)
        return imports, new_value

    raise RenderError(f"Unsupported type for key: {key}, value: {type(value)}")
