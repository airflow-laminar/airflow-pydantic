from functools import lru_cache
from importlib.metadata import version
from importlib.util import find_spec


@lru_cache(1)
def _airflow_3():
    if find_spec("apache-airflow"):
        if version("apache-airflow") >= "3.0.0":
            return True
        else:
            return False
    return None


__all__ = ("_airflow_3",)
