from importlib import import_module
from types import FunctionType, MethodType
from typing import Annotated, Optional

from pydantic import BeforeValidator, PlainSerializer

__all__ = (
    "get_import_path",
    "serialize_path_as_string",
    "ImportPath",
    "CallablePath",
)


def get_import_path(path: str) -> type:
    if isinstance(path, type):
        return path
    elif isinstance(path, (FunctionType, MethodType)):
        return path
    if not isinstance(path, str):
        raise TypeError(path)
    module, call = path.rsplit(".", 1)
    return getattr(import_module(module), call)


def serialize_path_as_string(value: type) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "__module__") and hasattr(value, "__qualname__"):
        return f"{value.__module__}.{value.__qualname__}"
    if hasattr(value, "__name__"):
        return f"{value.__module__}.{value.__name__}"
    if hasattr(value, "__class__") and hasattr(value.__class__, "__name__"):
        return f"{value.__class__.__module__}.{value.__class__.__name__}"
    if hasattr(value, "__class__") and hasattr(value.__class__, "__qualname__"):
        return f"{value.__class__.__module__}.{value.__class__.__qualname__}"
    raise TypeError(f"Could not derive module and name for {value}")


ImportPath = Annotated[type, BeforeValidator(get_import_path), PlainSerializer(serialize_path_as_string, when_used="json", return_type=str)]
CallablePath = Annotated[object, BeforeValidator(get_import_path), PlainSerializer(serialize_path_as_string, when_used="json", return_type=str)]
