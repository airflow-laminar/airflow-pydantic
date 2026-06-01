from contextlib import contextmanager
from unittest.mock import MagicMock, patch

__all__ = ("pools", "variables")


@contextmanager
def pools(return_value=None, side_effect=None):
    with (
        patch("airflow_pydantic.airflow.Pool.get_pool") as get_pool_mock,
        patch("airflow_pydantic.airflow.Pool.create_or_update_pool") as pool_create_or_update_pool_mock,
        patch("airflow_pydantic.airflow.get_pool") as get_pool_func_mock,
        patch("airflow_pydantic.airflow.create_or_update_pool") as create_or_update_pool_mock,
        patch("airflow_pydantic.extras.balancer.balancer.get_pool") as balancer_get_pool_mock,
        patch("airflow_pydantic.extras.balancer.balancer.create_or_update_pool") as balancer_create_or_update_pool_mock,
        patch("airflow_pydantic.airflow.get_parsing_context") as context_mock,
    ):
        get_pool_mocks = (get_pool_mock, get_pool_func_mock, balancer_get_pool_mock)
        create_pool_mocks = (
            pool_create_or_update_pool_mock,
            create_or_update_pool_mock,
            balancer_create_or_update_pool_mock,
        )
        for pool_mock in get_pool_mocks + create_pool_mocks:
            pool_mock.return_value = return_value
        if side_effect:
            for pool_mock in get_pool_mocks:
                pool_mock.side_effect = side_effect
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
