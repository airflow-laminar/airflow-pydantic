"""Tests for pool-related functions in airflow.py - CLI and airflowctl fallbacks."""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow_pydantic.migration import _airflow_3

# Only run these tests if we're in Airflow 3 mode
pytestmark = pytest.mark.skipif(
    _airflow_3() is not True,
    reason="Pool fallback functions only exist in Airflow 3",
)


class TestIsDatabaseAvailable:
    """Tests for _is_database_available function."""

    def test_returns_false_when_process_context_is_client(self):
        """Test that database is not available when in client (parsing) context."""
        from airflow_pydantic.airflow import _is_database_available

        with patch.dict("os.environ", {"_AIRFLOW_PROCESS_CONTEXT": "client"}):
            assert _is_database_available() is False

    def test_returns_false_when_engine_is_none(self):
        """Test that database is not available when engine is None."""
        from airflow_pydantic.airflow import _is_database_available

        with patch.dict("os.environ", {"_AIRFLOW_PROCESS_CONTEXT": ""}, clear=False):
            with patch("airflow.settings") as mock_settings:
                mock_settings.engine = None
                assert _is_database_available() is False

    def test_returns_false_when_session_is_none(self):
        """Test that database is not available when Session is None."""
        from airflow_pydantic.airflow import _is_database_available

        with patch.dict("os.environ", {"_AIRFLOW_PROCESS_CONTEXT": ""}, clear=False):
            with patch("airflow.settings") as mock_settings:
                mock_settings.engine = MagicMock()
                mock_settings.Session = None
                assert _is_database_available() is False

    def test_returns_false_when_query_fails(self):
        """Test that database is not available when query fails."""
        from airflow_pydantic.airflow import _is_database_available

        with patch.dict("os.environ", {"_AIRFLOW_PROCESS_CONTEXT": ""}, clear=False):
            with patch("airflow.settings") as mock_settings:
                mock_settings.engine = MagicMock()
                mock_session = MagicMock()
                mock_session.__enter__ = MagicMock(return_value=mock_session)
                mock_session.__exit__ = MagicMock(return_value=False)
                mock_session.execute.side_effect = Exception("DB connection failed")
                mock_settings.Session.return_value = mock_session
                assert _is_database_available() is False

    def test_returns_true_when_database_is_available(self):
        """Test that database is available when all checks pass."""
        from airflow_pydantic.airflow import _is_database_available

        with patch.dict("os.environ", {"_AIRFLOW_PROCESS_CONTEXT": ""}, clear=False):
            with patch("airflow.settings") as mock_settings:
                mock_settings.engine = MagicMock()
                mock_session = MagicMock()
                mock_session.__enter__ = MagicMock(return_value=mock_session)
                mock_session.__exit__ = MagicMock(return_value=False)
                mock_settings.Session.return_value = mock_session
                assert _is_database_available() is True


class TestGetPoolViaCli:
    """Tests for _get_pool_via_cli function."""

    def test_returns_pool_on_success(self):
        """Test that pool is returned when CLI succeeds."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([{"pool": "test_pool", "slots": 10, "description": "Test", "include_deferred": True}])

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            pool = _get_pool_via_cli("test_pool")

            assert pool is not None
            assert pool.pool == "test_pool"
            assert pool.slots == 10
            assert pool.description == "Test"
            assert pool.include_deferred is True

            mock_run.assert_called_once_with(
                ["airflow", "pools", "get", "test_pool", "--output", "json"],
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_returns_none_on_nonzero_exit(self):
        """Test that None is returned when CLI returns non-zero exit code."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("subprocess.run", return_value=mock_result):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None

    def test_returns_none_on_empty_output(self):
        """Test that None is returned when CLI returns empty output."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""

        with patch("subprocess.run", return_value=mock_result):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None

    def test_returns_none_on_empty_json_array(self):
        """Test that None is returned when CLI returns empty JSON array."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "[]"

        with patch("subprocess.run", return_value=mock_result):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None

    def test_returns_none_on_timeout(self):
        """Test that None is returned when CLI times out."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="airflow", timeout=30)):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None

    def test_returns_none_on_file_not_found(self):
        """Test that None is returned when airflow CLI is not found."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        with patch("subprocess.run", side_effect=FileNotFoundError()):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None

    def test_returns_none_on_json_decode_error(self):
        """Test that None is returned when CLI returns invalid JSON."""
        from airflow_pydantic.airflow import _get_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not valid json"

        with patch("subprocess.run", return_value=mock_result):
            pool = _get_pool_via_cli("test_pool")
            assert pool is None


class TestCreatePoolViaCli:
    """Tests for _create_pool_via_cli function."""

    def test_returns_true_on_success(self):
        """Test that True is returned when pool is created successfully."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = _create_pool_via_cli("test_pool", 10, "Test description", False)

            assert result is True
            mock_run.assert_called_once_with(
                ["airflow", "pools", "set", "test_pool", "10", "Test description"],
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_includes_deferred_flag_when_true(self):
        """Test that --include-deferred is added when include_deferred is True."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = _create_pool_via_cli("test_pool", 10, "Test", True)

            assert result is True
            mock_run.assert_called_once_with(
                ["airflow", "pools", "set", "test_pool", "10", "Test", "--include-deferred"],
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_handles_empty_description(self):
        """Test that empty description is handled correctly."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = _create_pool_via_cli("test_pool", 10, "", False)

            assert result is True
            mock_run.assert_called_once_with(
                ["airflow", "pools", "set", "test_pool", "10", ""],
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_returns_false_on_nonzero_exit(self):
        """Test that False is returned when CLI returns non-zero exit code."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error creating pool"

        with patch("subprocess.run", return_value=mock_result):
            result = _create_pool_via_cli("test_pool", 10, "Test", False)
            assert result is False

    def test_returns_false_on_timeout(self):
        """Test that False is returned when CLI times out."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="airflow", timeout=30)):
            result = _create_pool_via_cli("test_pool", 10, "Test", False)
            assert result is False

    def test_returns_false_on_file_not_found(self):
        """Test that False is returned when airflow CLI is not found."""
        from airflow_pydantic.airflow import _create_pool_via_cli

        with patch("subprocess.run", side_effect=FileNotFoundError()):
            result = _create_pool_via_cli("test_pool", 10, "Test", False)
            assert result is False


class TestGetPoolViaAirflowctl:
    """Tests for _get_pool_via_airflowctl function."""

    def test_returns_pool_on_success(self):
        """Test that pool is returned when airflowctl succeeds."""
        from airflow_pydantic.airflow import _get_pool_via_airflowctl

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([{"name": "test_pool", "slots": 10, "description": "Test", "include_deferred": True}])

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            pool = _get_pool_via_airflowctl("test_pool")

            assert pool is not None
            assert pool.pool == "test_pool"
            assert pool.slots == 10
            assert pool.description == "Test"
            assert pool.include_deferred is True

            mock_run.assert_called_once_with(
                ["airflowctl", "pools", "get", "test_pool", "--output", "json"],
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_returns_none_on_nonzero_exit(self):
        """Test that None is returned when airflowctl returns non-zero exit code."""
        from airflow_pydantic.airflow import _get_pool_via_airflowctl

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("subprocess.run", return_value=mock_result):
            pool = _get_pool_via_airflowctl("test_pool")
            assert pool is None

    def test_returns_none_on_file_not_found(self):
        """Test that None is returned when airflowctl is not found."""
        from airflow_pydantic.airflow import _get_pool_via_airflowctl

        with patch("subprocess.run", side_effect=FileNotFoundError()):
            pool = _get_pool_via_airflowctl("test_pool")
            assert pool is None


class TestCreatePoolViaAirflowctl:
    """Tests for _create_pool_via_airflowctl function."""

    def test_returns_true_on_success(self):
        """Test that True is returned when pool is created successfully."""
        from airflow_pydantic.airflow import _create_pool_via_airflowctl

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                mock_file = MagicMock()
                mock_file.name = "/tmp/test_pool.json"
                mock_file.__enter__ = MagicMock(return_value=mock_file)
                mock_file.__exit__ = MagicMock(return_value=False)
                mock_tempfile.return_value = mock_file

                with patch("os.unlink") as mock_unlink:
                    result = _create_pool_via_airflowctl("test_pool", 10, "Test", False)

                    assert result is True
                    mock_run.assert_called_once_with(
                        ["airflowctl", "pools", "import", "/tmp/test_pool.json"],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    mock_unlink.assert_called_once_with("/tmp/test_pool.json")

    def test_returns_false_on_nonzero_exit(self):
        """Test that False is returned when airflowctl returns non-zero exit code."""
        from airflow_pydantic.airflow import _create_pool_via_airflowctl

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error creating pool"

        with patch("subprocess.run", return_value=mock_result):
            with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                mock_file = MagicMock()
                mock_file.name = "/tmp/test_pool.json"
                mock_file.__enter__ = MagicMock(return_value=mock_file)
                mock_file.__exit__ = MagicMock(return_value=False)
                mock_tempfile.return_value = mock_file

                with patch("os.unlink"):
                    result = _create_pool_via_airflowctl("test_pool", 10, "Test", False)
                    assert result is False

    def test_returns_false_on_file_not_found(self):
        """Test that False is returned when airflowctl is not found."""
        from airflow_pydantic.airflow import _create_pool_via_airflowctl

        with patch("subprocess.run", side_effect=FileNotFoundError()):
            result = _create_pool_via_airflowctl("test_pool", 10, "Test", False)
            assert result is False


class TestGetPoolFallback:
    """Tests for get_pool function with fallback behavior."""

    def test_uses_direct_db_when_available(self):
        """Test that direct DB access is used when available."""
        from airflow_pydantic.airflow import get_pool

        mock_client = MagicMock()
        mock_client.get_pool.return_value = ("test_pool", 10, "Test", True)

        with patch("airflow_pydantic.airflow._is_database_available", return_value=True):
            with patch("airflow_pydantic.airflow.get_current_api_client", return_value=mock_client):
                pool = get_pool("test_pool")

                assert pool.pool == "test_pool"
                assert pool.slots == 10
                mock_client.get_pool.assert_called_once_with(name="test_pool")

    def test_falls_back_to_cli_when_db_unavailable(self):
        """Test that CLI is used when DB is unavailable."""
        from airflow_pydantic.airflow import get_pool

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([{"pool": "test_pool", "slots": 10, "description": "Test", "include_deferred": False}])

        with patch("airflow_pydantic.airflow._is_database_available", return_value=False):
            with patch("subprocess.run", return_value=mock_result):
                pool = get_pool("test_pool")

                assert pool.pool == "test_pool"
                assert pool.slots == 10

    def test_falls_back_to_airflowctl_when_cli_fails(self):
        """Test that airflowctl is used when CLI fails."""
        from airflow_pydantic.airflow import get_pool

        # First call is CLI (fails), second is airflowctl (succeeds)
        def run_side_effect(*args, **kwargs):
            cmd = args[0]
            mock_result = MagicMock()
            if cmd[0] == "airflow":
                mock_result.returncode = 1
                mock_result.stdout = ""
            else:  # airflowctl
                mock_result.returncode = 0
                mock_result.stdout = json.dumps([{"name": "test_pool", "slots": 10, "description": "Test", "include_deferred": False}])
            return mock_result

        with patch("airflow_pydantic.airflow._is_database_available", return_value=False):
            with patch("subprocess.run", side_effect=run_side_effect):
                pool = get_pool("test_pool")

                assert pool.pool == "test_pool"
                assert pool.slots == 10

    def test_raises_pool_not_found_when_all_methods_fail(self):
        """Test that PoolNotFound is raised when all methods fail."""
        from airflow_pydantic.airflow import PoolNotFound, get_pool

        with patch("airflow_pydantic.airflow._is_database_available", return_value=False):
            with patch("subprocess.run", side_effect=FileNotFoundError()):
                with pytest.raises(PoolNotFound):
                    get_pool("test_pool")


class TestCreateOrUpdatePoolFallback:
    """Tests for create_or_update_pool function with fallback behavior."""

    def test_uses_direct_db_when_available(self):
        """Test that direct DB access is used when available."""
        from airflow_pydantic.airflow import create_or_update_pool

        mock_client = MagicMock()
        mock_client.get_pool.return_value = ("test_pool", 10, "Test", False)

        with patch("airflow_pydantic.airflow._is_database_available", return_value=True):
            with patch("airflow_pydantic.airflow.get_current_api_client", return_value=mock_client):
                pool = create_or_update_pool("test_pool", slots=10, description="Test")

                assert pool is not None
                mock_client.create_pool.assert_called_once_with(name="test_pool", slots=10, description="Test", include_deferred=False)

    def test_falls_back_to_cli_when_db_unavailable(self):
        """Test that CLI is used when DB is unavailable."""
        from airflow_pydantic.airflow import create_or_update_pool

        mock_create_result = MagicMock()
        mock_create_result.returncode = 0

        mock_get_result = MagicMock()
        mock_get_result.returncode = 0
        mock_get_result.stdout = json.dumps([{"pool": "test_pool", "slots": 10, "description": "Test", "include_deferred": False}])

        call_count = [0]

        def run_side_effect(*args, **kwargs):
            call_count[0] += 1
            cmd = args[0]
            if "set" in cmd:
                return mock_create_result
            else:
                return mock_get_result

        with patch("airflow_pydantic.airflow._is_database_available", return_value=False):
            with patch("subprocess.run", side_effect=run_side_effect):
                pool = create_or_update_pool("test_pool", slots=10, description="Test")

                assert pool is not None
                assert pool.pool == "test_pool"

    def test_returns_none_when_all_methods_fail(self):
        """Test that None is returned when all methods fail."""
        from airflow_pydantic.airflow import create_or_update_pool

        with patch("airflow_pydantic.airflow._is_database_available", return_value=False):
            with patch("subprocess.run", side_effect=FileNotFoundError()):
                pool = create_or_update_pool("test_pool", slots=10, description="Test")

                assert pool is None
