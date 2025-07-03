from fnmatch import fnmatch
from logging import getLogger
from random import choice
from typing import Callable, List, Literal, Optional, Union

from pkn.pydantic import CallablePath
from pydantic import Field

from ...core import BaseModel
from .balancer import BalancerConfiguration
from .host import Host

__all__ = (
    "BalancerHostQueryConfiguration",
    "BalancerPortQueryConfiguration",
    "HostQuery",
    "PortQuery",
)

_log = getLogger(__name__)


class BalancerHostQueryConfiguration(BaseModel):
    kind: Literal["filter", "select"] = Field(
        default="select",
        description="Kind of query to perform, either 'filter' to return a list of matching hosts or 'select' to return a single host.",
    )
    balancer: BalancerConfiguration
    name: Optional[Union[str, List[str]]] = None
    queue: Optional[Union[str, List[str]]] = None
    os: Optional[Union[str, List[str]]] = None
    tag: Optional[Union[str, List[str]]] = None
    custom: Optional[Union[Callable, CallablePath]] = None

    def execute(
        self,
    ) -> Union[List[Host], Host]:
        """
        Execute the query against the provided hosts and ports.
        """
        hosts = self.balancer.all_hosts
        name = self.name or []
        queue = self.queue or []
        os = self.os or []
        tag = self.tag or []
        if isinstance(name, str):
            name = [name]
        if isinstance(queue, str):
            queue = [queue]
        if isinstance(os, str):
            os = [os]
        if isinstance(tag, str):
            tag = [tag]

        candidates = [
            host
            for host in hosts
            if (not name or any(fnmatch(host.name, n) for n in name))
            and (not queue or any(fnmatch(host_queue, queue_pat) for queue_pat in queue for host_queue in host.queues))
            and (not tag or any(fnmatch(host_tag, tag_pat) for tag_pat in tag for host_tag in host.tags))
            and (not os or any(fnmatch(host.os, o) for o in os))
            and (not self.custom or self.custom(host))
        ]

        if not candidates:
            raise RuntimeError(f"No host found for {name} / {queue} / {os} / {tag}")

        if self.kind == "filter":
            _log.info(f"Filtered hosts: {len(candidates)} found")
            return candidates
        # TODO more schemes, interrogate usage
        ret = choice(candidates)
        _log.info(f"Selected host: {ret.name} ({ret.os})")
        return ret


# Alias
HostQuery = BalancerHostQueryConfiguration


class BalancerPortQueryConfiguration(BaseModel):
    kind: Literal["filter", "select"] = Field(
        default="select",
        description="Kind of query to perform, either 'filter' to return a list of matching hosts or 'select' to return a single host.",
    )
    balancer: BalancerConfiguration
    name: Optional[Union[str, List[str]]] = None
    tag: Optional[Union[str, List[str]]] = None
    custom: Optional[Union[Callable, CallablePath]] = None

    def execute(
        self,
    ) -> Union[List[Host], Host]:
        """
        Execute the query against the provided hosts and ports.
        """
        ports = self.balancer.all_ports
        name = self.name or []
        tag = self.tag or []
        if isinstance(name, str):
            name = [name]
        if isinstance(tag, str):
            tag = [tag]

        candidates = [
            port
            for port in ports
            if (not name or any(fnmatch(port.name, n) for n in name))
            and (not tag or any(fnmatch(port_tag, tag_pat) for tag_pat in tag for port_tag in port.tags))
            and (not self.custom or self.custom(port))
        ]

        if not candidates:
            raise RuntimeError(f"No port found for {name} / {tag}")

        if self.kind == "filter":
            _log.info(f"Filtered ports: {len(candidates)} found")
            return candidates
        # TODO more schemes, interrogate usage
        ret = choice(candidates)
        _log.info(f"Selected port: {ret._calc_name}")
        return ret


# Alias
PortQuery = BalancerPortQueryConfiguration
