from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from importlib.util import find_spec

__all__ = ("_airflow_3",)


@lru_cache(1)
def _airflow_3():
    # NOTE: sometimes airflow2/3 mixing causes issues,
    # so check both apache-airflow and airflow packages
    try:
        return (find_spec("apache-airflow") or find_spec("airflow")) and version("apache-airflow") >= "3.0.0"
    except PackageNotFoundError:
        # Sometimes metadata is corrupted
        pass
    return None


__all__ = ("_airflow_3",)
