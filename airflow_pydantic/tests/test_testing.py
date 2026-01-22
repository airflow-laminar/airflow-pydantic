"""Tests for the testing utilities module."""

from unittest.mock import MagicMock

from airflow_pydantic.testing import pools, variables


class TestTestingUtils:
    """Tests for testing utility context managers."""

    def test_pools_context_manager(self):
        """Test the pools context manager."""
        mock_pool = MagicMock()
        mock_pool.pool = "test-pool"
        mock_pool.slots = 5

        with pools(return_value=mock_pool) as get_pool_mock:
            # The mock should be configured
            assert get_pool_mock.return_value == mock_pool

    def test_pools_with_side_effect(self):
        """Test the pools context manager with side_effect."""
        test_error = Exception("Pool not found")

        with pools(side_effect=test_error) as get_pool_mock:
            # The mock should have side_effect configured
            assert get_pool_mock.side_effect == test_error

    def test_pools_with_none(self):
        """Test the pools context manager with None return value."""
        with pools(return_value=None) as get_pool_mock:
            assert get_pool_mock.return_value is None

    def test_variables_context_manager(self):
        """Test the variables context manager."""
        with variables(return_value="test_value") as get_mock:
            assert get_mock.return_value == "test_value"

    def test_variables_with_side_effect(self):
        """Test the variables context manager with side_effect."""
        test_error = KeyError("Variable not found")

        with variables(side_effect=test_error) as get_mock:
            assert get_mock.side_effect == test_error

    def test_variables_with_dict(self):
        """Test the variables context manager returning a dict."""
        config = {"key1": "value1", "key2": "value2"}

        with variables(return_value=config) as get_mock:
            assert get_mock.return_value == config

    def test_variables_with_none(self):
        """Test the variables context manager with None return value."""
        with variables(return_value=None) as get_mock:
            assert get_mock.return_value is None

    def test_pools_context_manager_patches_correctly(self):
        """Test that pools patches the correct functions."""
        with pools() as get_pool_mock:
            # Just verify we can use the context manager without error
            assert get_pool_mock is not None

    def test_variables_context_manager_patches_correctly(self):
        """Test that variables patches the correct function."""
        with variables() as get_mock:
            # Just verify we can use the context manager without error
            assert get_mock is not None
