from shlex import quote as shell_quote
from typing import Dict, List, Optional

from pydantic import BaseModel, model_serializer

__all__ = ("BashCommands", "in_bash")


def in_bash(
    command: str,
    quote: Optional[str] = "'",
    escape: Optional[bool] = False,
    login: Optional[bool] = True,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> str:
    """Run command inside bash.

    Args:
        command (str): string command to run
        quote (str, optional): Optional simple quoting, without escaping. May cause mismatched quote problems. Defaults to "'".
        escape (bool, optional): Full shell escaping. Defaults to False.
        login (bool, optional): Run in login shell (-l). Defaults to True.
        cwd (str, optional): Working directory to run the command in. Defaults to None.

    Returns:
        str: String command to run, starts with "bash"
    """
    if cwd:
        command = f"cd {shell_quote(cwd)}\n{command}"
    if escape:
        command = shell_quote(command)
    if env:
        for key, value in env.items():
            # NOTE: don't quote env as they may be airflow vars
            command = f'export {key}="{value}"\n{command}'
    if quote:
        command = f"{quote}{command}{quote}"
    bash_flags = "-lc" if login else "-c"
    return f"bash {bash_flags} {command}"


class BashCommands(BaseModel):
    commands: List[str]
    quote: Optional[str] = "'"
    escape: Optional[bool] = False
    login: Optional[bool] = True
    cwd: Optional[str] = ""
    env: Optional[Dict[str, str]] = None

    @model_serializer()
    def _serialize(self) -> str:
        return in_bash("\n".join(["set -ex"] + self.commands), quote=self.quote, escape=self.escape, login=self.login, cwd=self.cwd, env=self.env)
