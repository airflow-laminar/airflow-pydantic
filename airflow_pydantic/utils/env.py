from .bash import in_bash

__all__ = ("in_bash", "in_conda", "in_virtualenv")


def in_conda(env: str, command: str, tool: str = "micromamba") -> str:
    return f"{tool} activate {env} && {command}"


def in_virtualenv(env: str, command: str) -> str:
    return f"source {env}/bin/activate && {command}"
