from logging import getLogger
from pathlib import Path
from random import choice
from typing import Callable, List, Optional, Union

from pydantic import Field, model_validator

try:
    from sqlalchemy.exc import OperationalError
except ImportError:
    OperationalError = RuntimeError

from typing_extensions import Self

from ...core import BaseModel
from ...utils import Pool, Variable
from .host import Host
from .port import Port

__all__ = ("BalancerConfiguration", "load_config")

_log = getLogger(__name__)


class BalancerConfiguration(BaseModel):
    hosts: List[Host] = Field(default_factory=list)
    ports: List[Port] = Field(default_factory=list)

    default_username: str = "airflow"

    # Password
    default_password: Optional[Union[str, Variable]] = None

    # Or get key file
    default_key_file: Optional[str] = None

    # The queue that might include the host running airflow itself
    primary_queue: str = "default"

    # The queue that does not include the host running airflow itself
    secondary_queue: str = "default"

    # The default worker queue
    default_queue: str = "default"

    # The default pool size
    default_size: int = Field(default=8)

    # rewrite pool size from config if differs from airflow variable stored value
    override_pool_size: bool = False

    # create connection object in airflow for host
    create_connection: bool = False

    @property
    def all_hosts(self):
        return sorted(list(set(self.hosts)))

    @property
    def all_ports(self):
        return sorted(list(set(self.ports)))

    @model_validator(mode="after")
    def _validate(self) -> Self:
        from airflow_pydantic.airflow import Pool as AirflowPool, PoolNotFound, get_parsing_context

        # Validate no duplicate hosts
        seen_hostnames = set()
        for host in self.hosts:
            if host.name in seen_hostnames:
                raise ValueError(f"Duplicate host found: {host.name}")
            seen_hostnames.add(host.name)

        # Handle limits
        for host in self.hosts:
            if not host.size:
                host.size = self.default_size
            if not host.pool:
                host.pool = Pool(
                    pool=host.name,
                    slots=host.size,
                    description=f"Balancer pool for host({host.name})",
                )

            if get_parsing_context().dag_id is not None:
                # check airflow first
                try:
                    if isinstance(host.pool, Pool):
                        pool_name = host.pool.pool
                    elif isinstance(host.pool, AirflowPool):
                        pool_name = host.pool.pool
                    elif isinstance(host.pool, dict):
                        pool_name = host.pool.get("pool")
                    elif isinstance(host.pool, str):
                        pool_name = host.pool

                    res = AirflowPool.get_pool(pool_name)

                    # airflow return value differs version-to-version
                    if res is None:
                        raise PoolNotFound
                    elif res.slots != host.size:
                        if self.override_pool_size:
                            AirflowPool.create_or_update_pool(
                                name=pool_name,
                                slots=host.size,
                                description=host.pool.description,
                                include_deferred=False,
                            )
                        else:
                            host.size = res.slots
                except PoolNotFound:
                    try:
                        # else set to default
                        AirflowPool.create_or_update_pool(
                            name=host.name,
                            slots=host.size,
                            description=f"Balancer pool for host({host.name})",
                            include_deferred=False,
                        )
                    except OperationalError:
                        # If the database is not available, we cannot create the pool
                        pass

            if not host.username and self.default_username:
                host.username = self.default_username
            if not host.password and self.default_password:
                host.password = self.default_password
            if not host.key_file and self.default_key_file:
                host.key_file = self.default_key_file
            if not host.size:
                host.size = self.default_size

        # Handle ports
        _used_ports = set()
        for port in self.ports:
            if port.host_name and not port.host:
                port.host = next((host for host in self.all_hosts if host.name == port.host_name), None)
            if not port.port:
                raise ValueError("Port must be specified")
            if not port.host:
                raise ValueError("Host must be specified")
            if (port.host.name, port.port) in _used_ports:
                raise ValueError(f"Duplicate port usage for host: {port.host.name}:{port.port}")
            _used_ports.add((port.host.name, port.port))

            # Create pools
            # TODO reenable
            # Pool.create_or_update_pool(
            #     name=port.pool,
            #     slots=1,
            #     description=f"Balancer pool for host({port.port}) port({port.port})",
            #     include_deferred=True,
            # )

        # sort hosts by name, sort ports by host name then port number
        # NOTE: since we're in a validator and we have validate_on_assignment, bypass here
        self.__dict__["hosts"] = sorted(self.hosts, key=lambda host: host.name)
        self.__dict__["ports"] = sorted(self.ports, key=lambda port: (port.host.name, port.port))
        return self

    def filter_hosts(
        self,
        name: Optional[Union[str, List[str]]] = None,
        queue: Optional[Union[str, List[str]]] = None,
        os: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        custom: Optional[Callable] = None,
    ) -> List[Host]:
        from .query import BalancerHostQueryConfiguration

        query = BalancerHostQueryConfiguration(
            kind="filter",
            name=name,
            queue=queue,
            os=os,
            tag=tag,
            custom=custom,
            balancer=self,
        )
        return query.execute()

    def select_host(
        self,
        name: Optional[Union[str, List[str]]] = None,
        queue: Optional[Union[str, List[str]]] = None,
        os: Union[str, List[str]] = "",
        tag: Union[str, List[str]] = "",
        custom: Callable = None,
    ) -> List[Host]:
        from .query import BalancerHostQueryConfiguration

        query = BalancerHostQueryConfiguration(
            kind="select",
            name=name,
            queue=queue,
            os=os,
            tag=tag,
            custom=custom,
            balancer=self,
        )
        return query.execute()

    def filter_ports(
        self,
        name: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        custom: Optional[Callable] = None,
    ) -> List[Host]:
        from .query import BalancerPortQueryConfiguration

        query = BalancerPortQueryConfiguration(
            kind="filter",
            name=name,
            tag=tag,
            custom=custom,
            balancer=self,
        )
        return query.execute()

    def select_port(
        self,
        name: Optional[Union[str, List[str]]] = None,
        tag: Union[str, List[str]] = "",
        custom: Callable = None,
    ) -> List[Host]:
        from .query import BalancerPortQueryConfiguration

        query = BalancerPortQueryConfiguration(
            kind="select",
            name=name,
            tag=tag,
            custom=custom,
            balancer=self,
        )
        return query.execute()

    def free_port(
        self,
        host: Host,
        min: int = 1000,
        max: int = 65535,
    ) -> Port:
        used_ports = [port.port for port in self.ports if port.host == host]
        port = Port(host=host, port=choice(range(min, max)))
        while port.port in used_ports:
            port = Port(host=host, port=choice(range(min, max)))
            # TODO add pool around port? or just allow? context manager?
        return port

    @staticmethod
    def load_path(yaml_file: str | Path, _config_dir: str | Path = None) -> Self:
        """Load configuration from yaml file"""
        from hydra import compose, initialize_config_dir
        from hydra.utils import instantiate

        if not isinstance(yaml_file, Path):
            yaml_file = Path(yaml_file).resolve()
        if not yaml_file.suffix == ".yaml":
            raise ValueError(f"File {yaml_file} must end in .yaml")

        file_name = yaml_file.stem
        config_dir = str(_config_dir or yaml_file.parent)

        # TODO: how to get the underlying value directly
        with initialize_config_dir(config_dir=config_dir, version_base=None):
            cfg = compose(config_name=file_name, overrides=[])
            inst_cfg = instantiate(cfg)

            # Try to find the BalancerConfiguration in the config
            # TODO: recursively search for BalancerConfiguration
            # TODO: more than one?
            if yaml_file.parent.stem in inst_cfg:
                config = inst_cfg[yaml_file.parent.stem]
            else:
                config = inst_cfg

            if "extensions" in config:
                config = config["extensions"]

            if not isinstance(config, BalancerConfiguration):
                # Try to look through values again
                for value in config.values():
                    if isinstance(value, BalancerConfiguration):
                        config = value
                        break
                else:
                    raise ValueError(f"Config {file_name} does not contain a BalancerConfiguration")
            return config

    @staticmethod
    def load(
        config_dir: Path | str = "config",
        config_name: Path | str = "",
        overrides: Optional[list[str]] = None,
        *,
        basepath: str = "",
        _offset: int = 4,
    ) -> "BalancerConfiguration":
        from airflow_config import load_config as load_airflow_config
        from hydra.errors import HydraException

        try:
            _log.info(f"Loading balancer configuration from {config_dir} with name {config_name}")
            cfg = load_airflow_config(
                config_dir=str(config_dir),
                config_name=str(config_name),
                overrides=overrides,
                basepath=basepath,
                _offset=_offset,
            )
            if cfg.extensions:
                for ext in cfg.extensions.values():
                    if isinstance(ext, BalancerConfiguration):
                        return ext
        except HydraException:
            pass
        _log.warning(f"Balancer configuration not found in {config_dir} with name {config_name}, loading default")
        return BalancerConfiguration.load_path(
            yaml_file=Path(config_dir) / f"{config_name or 'balancer'}.yaml",
            _config_dir=config_dir,
        )


load_config = BalancerConfiguration.load
