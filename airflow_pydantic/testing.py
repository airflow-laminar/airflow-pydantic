from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from .migration import _airflow_3

__all__ = ("pools", "variables")


if _airflow_3():

    @contextmanager
    def pools(return_value=None, side_effect=None):
        mock_client = MagicMock()
        mock_pool_resp = MagicMock()
        if return_value is not None:
            mock_pool_resp.pool = getattr(return_value, "pool", return_value)
            mock_pool_resp.slots = getattr(return_value, "slots", 0)
            mock_pool_resp.description = getattr(return_value, "description", "")
            mock_pool_resp.include_deferred = getattr(return_value, "include_deferred", False)
        mock_client.pools.get.return_value = mock_pool_resp
        mock_client.pools.create.return_value = mock_pool_resp
        mock_client.pools.update.return_value = mock_pool_resp
        if side_effect:
            mock_client.pools.get.side_effect = side_effect

        with (
            patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client) as client_mock,
            patch("airflow_pydantic.airflow.get_parsing_context") as context_mock,
        ):
            context_mock.return_value = MagicMock()
            context_mock.return_value.dag_id = "airflow_pydantic.testing.pools"
            yield client_mock

else:

    @contextmanager
    def pools(return_value=None, side_effect=None):
        with (
            patch("airflow_pydantic.airflow.Pool.get_pool") as get_pool_mock,
            patch("airflow_pydantic.airflow.Pool.create_or_update_pool") as pool_create_or_update_pool_mock,
            patch("airflow_pydantic.airflow.create_or_update_pool") as create_or_update_pool_mock,
            patch("airflow_pydantic.airflow.get_parsing_context") as context_mock,
        ):
            get_pool_mock.return_value = return_value
            pool_create_or_update_pool_mock.return_value = return_value
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
