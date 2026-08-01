def test_all():
    from airflow_pydantic import BaseModel, Dag, Task  # noqa: F401


def test_reexports():
    from pkn import Dict, List

    from airflow_pydantic import Dict as ADict, List as AList

    assert Dict is ADict
    assert List is AList
