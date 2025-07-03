from subprocess import call
from typing import Callable

from ..airflow import AirflowSkipException

__all__ = ("ping",)


def ping(host, *, local=True) -> Callable:
    if host != "localhost" and host.count(".") == 0 and local:
        host = f"{host}.local"

    def _ping(hostname=host):
        if call(["ping", "-c", "1", f"{hostname}"]) == 0:
            return True
        raise AirflowSkipException

    return _ping
