"""Tests for pool-related functions in airflow.py - LocalRESTClient-based."""

from unittest.mock import MagicMock, patch

import pytest

from airflow_pydantic.migration import _airflow_3

# Only run these tests if we're in Airflow 3 mode
pytestmark = pytest.mark.skipif(
    _airflow_3() is not True,
    reason="Pool LocalRESTClient functions only exist in Airflow 3",
)


class TestGetPool:
    """Tests for get_pool function using LocalRESTClient."""

    def test_returns_pool_on_success(self):
        """Test that pool is returned when client succeeds."""
        from airflow_pydantic.airflow import get_pool

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.pool = "test_pool"
        mock_resp.slots = 10
        mock_resp.description = "Test"
        mock_resp.include_deferred = True
        mock_client.pools.get.return_value = mock_resp

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client):
            pool = get_pool("test_pool")

            assert pool.pool == "test_pool"
            assert pool.slots == 10
            assert pool.description == "Test"
            assert pool.include_deferred is True
            mock_client.pools.get.assert_called_once_with("test_pool")

    def test_raises_pool_not_found_on_404(self):
        """Test that PoolNotFound is raised when client returns 404."""
        import httpx

        from airflow_pydantic.airflow import PoolNotFound, get_pool

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.pools.get.side_effect = httpx.HTTPStatusError("Not found", request=MagicMock(), response=mock_response)

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client), pytest.raises(PoolNotFound):
            get_pool("nonexistent_pool")

    def test_reraises_non_404_errors(self):
        """Test that non-404 HTTP errors are re-raised."""
        import httpx

        from airflow_pydantic.airflow import get_pool

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client.pools.get.side_effect = httpx.HTTPStatusError("Server error", request=MagicMock(), response=mock_response)

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client), pytest.raises(httpx.HTTPStatusError):
            get_pool("test_pool")

    def test_handles_none_description(self):
        """Test that None description is converted to empty string."""
        from airflow_pydantic.airflow import get_pool

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.pool = "test_pool"
        mock_resp.slots = 5
        mock_resp.description = None
        mock_resp.include_deferred = False
        mock_client.pools.get.return_value = mock_resp

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client):
            pool = get_pool("test_pool")
            assert pool.description == ""


class TestCreateOrUpdatePool:
    """Tests for create_or_update_pool function using LocalRESTClient."""

    def test_updates_existing_pool(self):
        """Test that an existing pool is updated."""
        from airflow_pydantic.airflow import create_or_update_pool

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.pool = "test_pool"
        mock_resp.slots = 10
        mock_resp.description = "Updated"
        mock_resp.include_deferred = False
        mock_client.pools.update.return_value = mock_resp

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client):
            pool = create_or_update_pool("test_pool", slots=10, description="Updated")

            assert pool.pool == "test_pool"
            assert pool.slots == 10
            mock_client.pools.update.assert_called_once_with("test_pool", slots=10, description="Updated", include_deferred=False)
            mock_client.pools.create.assert_not_called()

    def test_creates_pool_when_update_fails(self):
        """Test that pool is created when update fails (pool doesn't exist)."""
        import httpx

        from airflow_pydantic.airflow import create_or_update_pool

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.pools.update.side_effect = httpx.HTTPStatusError("Not found", request=MagicMock(), response=mock_response)
        mock_resp = MagicMock()
        mock_resp.pool = "new_pool"
        mock_resp.slots = 5
        mock_resp.description = "New pool"
        mock_resp.include_deferred = True
        mock_client.pools.create.return_value = mock_resp

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client):
            pool = create_or_update_pool("new_pool", slots=5, description="New pool", include_deferred=True)

            assert pool.pool == "new_pool"
            assert pool.slots == 5
            assert pool.include_deferred is True
            mock_client.pools.create.assert_called_once_with(name="new_pool", slots=5, description="New pool", include_deferred=True)

    def test_handles_none_description(self):
        """Test that None/empty description is handled correctly."""
        from airflow_pydantic.airflow import create_or_update_pool

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.pool = "test_pool"
        mock_resp.slots = 10
        mock_resp.description = None
        mock_resp.include_deferred = False
        mock_client.pools.update.return_value = mock_resp

        with patch("airflow_pydantic.airflow.get_local_rest_client", return_value=mock_client):
            pool = create_or_update_pool("test_pool", slots=10)
            assert pool.description == ""
