from functools import lru_cache
from importlib.metadata import version
from importlib.util import find_spec

__all__ = ("_airflow_3",)


@lru_cache(1)
def _airflow_3():
    # NOTE: sometimes airflow2/3 mixing causes issues,
    # so check both apache-airflow and airflow packages
    if find_spec("apache-airflow") or find_spec("airflow"):
        if version("apache-airflow") >= "3.0.0":
            return True
        else:
            return False
    return None


__all__ = ("_airflow_3",)
