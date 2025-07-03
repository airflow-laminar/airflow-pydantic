from unittest.mock import patch

import pytest

from airflow_pydantic import in_bash, in_conda, in_virtualenv, ping
from airflow_pydantic.airflow import AirflowSkipException


class TestUtils:
    def test_in_bash(self):
        assert in_bash('a simple "test"', escape=False, quote="'", login=True) == "bash -lc 'a simple \"test\"'"
        assert in_bash("a simple 'test'", escape=True, quote=False, login=False) == "bash -c 'a simple '\"'\"'test'\"'\"''"

    def test_in_conda(self):
        assert in_conda("a", "b") == "micromamba activate a && b"

    def test_in_virtualenv(self):
        assert in_virtualenv("a", "b") == "source a/bin/activate && b"

    def test_ping(self):
        assert ping("localhost")()
        with pytest.raises(AirflowSkipException):
            ping("nonexistent")()

    def test_ping_localappend(self):
        with patch("airflow_pydantic.utils.host.call") as call:
            call.return_value = 0
            ping("blerg")()
            assert call.call_args[0][0] == ["ping", "-c", "1", "blerg.local"]
