import pytest

from airflow_pydantic import fail, pass_, skip
from airflow_pydantic.airflow import AirflowFailException, AirflowSkipException


class TestCommon:
    def test_pass(self):
        pass_()

    def test_skip(self):
        with pytest.raises(AirflowSkipException):
            skip()

    def test_fail(self):
        with pytest.raises(AirflowFailException):
            fail()
