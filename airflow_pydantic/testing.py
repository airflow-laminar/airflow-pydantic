from contextlib import contextmanager
from unittest.mock import MagicMock, patch

__all__ = ("pools", "variables")


@contextmanager
def pools(return_value=None, side_effect=None):
    with (
        patch("airflow_pydantic.airflow.Pool.get_pool") as get_pool_mock,
        patch("airflow_pydantic.airflow.Pool.create_or_update_pool") as create_or_update_pool_mock,
        patch("airflow_pydantic.airflow.get_parsing_context") as context_mock,
    ):
        get_pool_mock.return_value = return_value
        create_or_update_pool_mock.return_value = return_value
        if side_effect:
            get_pool_mock.side_effect = side_effect
        context_mock.return_value = MagicMock()
        context_mock.return_value.dag_id = "airflow_pydantic.testing.pools"
        yield get_pool_mock


@contextmanager
def variables(return_value=None, side_effect=None):
    with patch("airflow_pydantic.airflow.Variable.get") as get_mock:
        get_mock.return_value = return_value
        if side_effect:
            get_mock.side_effect = side_effect
        yield get_mock
