from shlex import quote as shell_quote
from typing import List

from pydantic import BaseModel, model_serializer

__all__ = ("BashCommands", "in_bash")


def in_bash(command: str, quote: str = "'", escape: bool = False, login: bool = True) -> str:
    """Run command inside bash.

    Args:
        command (str): string command to run
        quote (str, optional): Optional simple quoting, without escaping. May cause mismatched quote problems. Defaults to "'".
        escape (bool, optional): Full shell escaping. Defaults to False.
        login (bool, optional): Run in login shell (-l). Defaults to True.

    Returns:
        str: String command to run, starts with "bash"
    """
    if escape:
        command = shell_quote(command)
    if quote:
        command = f"{quote}{command}{quote}"
    if login:
        bash_flags = "-lc"
    else:
        bash_flags = "-c"
    return f"bash {bash_flags} {command}"


class BashCommands(BaseModel):
    commands: List[str]
    quote: str = "'"
    escape: bool = False
    login: bool = True

    @model_serializer()
    def _serialize(self) -> str:
        return in_bash("\n".join(["set -ex"] + self.commands), quote=self.quote, escape=self.escape, login=self.login)
