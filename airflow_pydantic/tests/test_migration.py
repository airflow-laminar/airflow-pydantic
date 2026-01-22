"""Tests for the migration module."""

from unittest.mock import patch

from airflow_pydantic.migration import _airflow_3


class TestMigration:
    """Tests for migration utilities."""

    def test_airflow_3_cached(self):
        """Test that _airflow_3 returns a cached result."""
        # Clear the cache first
        _airflow_3.cache_clear()

        result1 = _airflow_3()
        result2 = _airflow_3()

        # Should be the same cached result
        assert result1 is result2

    def test_airflow_3_returns_bool_or_none(self):
        """Test that _airflow_3 returns True, False, or None."""
        _airflow_3.cache_clear()
        result = _airflow_3()
        assert result in (True, False, None)

    def test_airflow_3_with_airflow_installed(self):
        """Test _airflow_3 when airflow is installed."""
        _airflow_3.cache_clear()

        with patch("airflow_pydantic.migration.find_spec") as mock_find_spec:
            with patch("airflow_pydantic.migration.version") as mock_version:
                mock_find_spec.return_value = True
                mock_version.return_value = "2.9.0"

                _airflow_3.cache_clear()
                result = _airflow_3()
                assert result is False

    def test_airflow_3_with_airflow3_installed(self):
        """Test _airflow_3 when airflow 3 is installed."""
        _airflow_3.cache_clear()

        with patch("airflow_pydantic.migration.find_spec") as mock_find_spec:
            with patch("airflow_pydantic.migration.version") as mock_version:
                mock_find_spec.return_value = True
                mock_version.return_value = "3.0.0"

                _airflow_3.cache_clear()
                result = _airflow_3()
                assert result is True

    def test_airflow_3_not_installed(self):
        """Test _airflow_3 when airflow is not installed."""
        _airflow_3.cache_clear()

        with patch("airflow_pydantic.migration.find_spec") as mock_find_spec:
            mock_find_spec.return_value = None

            _airflow_3.cache_clear()
            result = _airflow_3()
            assert result is None

    def test_airflow_3_package_not_found(self):
        """Test _airflow_3 when metadata is corrupted."""
        _airflow_3.cache_clear()

        from importlib.metadata import PackageNotFoundError

        with patch("airflow_pydantic.migration.find_spec") as mock_find_spec:
            with patch("airflow_pydantic.migration.version") as mock_version:
                mock_find_spec.return_value = True
                mock_version.side_effect = PackageNotFoundError("apache-airflow")

                _airflow_3.cache_clear()
                result = _airflow_3()
                assert result is None
