import pytest
from pydantic import ValidationError

from airflow_pydantic import BalancerConfiguration, Host, Port
from airflow_pydantic.airflow import PoolNotFound
from airflow_pydantic.testing import pools


class TestConfig:
    def test_no_duplicate_ports(self):
        with pools(side_effect=PoolNotFound()):
            h1 = Host(name="host1")
            h2 = Host(name="host2")
            p1 = Port(host=h1, port=1002)
            p2 = Port(name="p2", host=h2, port=1000)
            p3 = Port(name="p3", host=h2, port=999)

            with pytest.raises(ValidationError):
                Port()
            with pytest.raises(ValidationError):
                Port(host=Host(name="h1"), host_name="h2")

            p1dupe = Port(host_name="host1", port=1002)

            assert p2 < p3
            assert p2 == p2
            assert p1 == p1dupe
            assert hash(p1) != hash(p2)
            assert p1.pool == "host1-1002"
            assert p2.pool == "p2"
            assert p1dupe.pool == "host1-1002"

            BalancerConfiguration(
                hosts=[h1, h2],
                ports=[p1, p2],
            )

            with pytest.raises(ValueError):
                BalancerConfiguration(
                    hosts=[h1, h2],
                    ports=[p1, p2, p1dupe],
                )
