from __future__ import annotations

import ipaddress
import re
import uuid
from dataclasses import dataclass
from typing import Optional

_PROTOCOL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*\Z")
_REMOTE_NAME = re.compile(r"[A-Za-z0-9._~-]{1,255}\Z")
_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")


@dataclass(frozen=True)
class Address:
    protocol: str
    system: str
    host: Optional[str] = None
    port: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.protocol, str) or _PROTOCOL.fullmatch(self.protocol) is None:
            raise ValueError("Invalid actor path protocol")
        if not _is_remote_name(self.system):
            raise ValueError("Invalid actor system name")
        if self.host is None:
            if self.port is not None:
                raise ValueError("Actor path port requires a host")
            return
        if self.port is None:
            raise ValueError("Actor path host requires a port")
        canonical_host, canonical_port = _parse_endpoint(
            f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
        )
        if canonical_host != self.host or canonical_port != self.port:
            raise ValueError("Invalid actor path host or port")

    @property
    def has_local_scope(self) -> bool:
        return self.host is None

    @property
    def has_global_scope(self) -> bool:
        return self.host is not None

    def __str__(self) -> str:
        address = f"{self.protocol}://{self.system}"
        if self.host is not None:
            host = f"[{self.host}]" if ":" in self.host else self.host
            address += f"@{host}"
            if self.port is not None:
                address += f":{self.port}"
        return address


def split_name_and_uid(name: str) -> tuple[str, Optional[uuid.UUID]]:
    if "#" in name:
        plain_name, uid = name.rsplit("#", 1)
        return plain_name, uuid.UUID(uid)
    return name, None


class ActorPath:
    def __init__(
        self,
        address: Address,
        name: str,
        parent: Optional[ActorPath] = None,
        uid: Optional[uuid.UUID] = None,
    ) -> None:
        self._address = address
        self._name = name
        self._parent = parent
        self._uid = uid

    def child(self, child: str) -> ActorPath:
        raise NotImplementedError

    @property
    def address(self) -> Address:
        return self._address

    @property
    def name(self) -> str:
        return self._name

    def __truediv__(self, child: str) -> ActorPath:
        return self.child(child)

    def elements(self) -> list[str]:
        raise NotImplementedError

    @property
    def remote_path(self) -> str:
        return "/" + "/".join(self.elements()[1:])

    @property
    def canonical(self) -> str:
        return f"{self._address}{self.remote_path}"

    @property
    def is_remote_resolvable(self) -> bool:
        return (
            self._address.protocol == "movie"
            and _is_remote_name(self._address.system)
            and len(self.elements()) > 1
            and all(_is_remote_name(element) for element in self.elements()[1:])
        )

    def __repr__(self) -> str:
        return f"ActorPath({self.canonical!r})"

    def __str__(self) -> str:
        return self.canonical

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ActorPath) and self.canonical == other.canonical

    def __hash__(self) -> int:
        return hash(self.canonical)

    @staticmethod
    def parse(value: str) -> ActorPath:
        return parse_actor_path(value)


class RootActorPath(ActorPath):
    def __init__(self, address: Address) -> None:
        super().__init__(address, name="/", parent=None)
        self._parent = self

    def child(self, child: str) -> ActorPath:
        name, uid = split_name_and_uid(child)
        return ChildActorPath(name=name, parent=self, uid=uid)

    def elements(self) -> list[str]:
        return ["/"]


class ChildActorPath(ActorPath):
    def __init__(
        self, name: str, parent: ActorPath, uid: Optional[uuid.UUID] = None
    ) -> None:
        if "#" in name:
            raise ValueError("Name cannot contain '#' character")
        if "/" in name:
            raise ValueError("Name cannot contain '/' character")
        super().__init__(address=parent._address, name=name, parent=parent, uid=uid)

    def child(self, child: str) -> ActorPath:
        name, uid = split_name_and_uid(child)
        return ChildActorPath(name=name, parent=self, uid=uid)

    def elements(self) -> list[str]:
        elements = []
        current: Optional[ActorPath] = self
        while current is not None and current._parent is not current:
            elements.append(current._name)
            current = current._parent
        elements.append("/")
        elements.reverse()
        return elements


def parse_actor_path(value: str) -> ActorPath:
    if not isinstance(value, str):
        raise TypeError("Actor path must be a string")
    if not value.isascii() or "#" in value or "?" in value or "%" in value:
        raise ValueError("Actor path is not canonical")
    try:
        protocol, remainder = value.split("://", 1)
        authority, remote_path = remainder.split("/", 1)
    except ValueError as error:
        raise ValueError("Invalid actor path") from error
    if protocol != "movie" or not authority:
        raise ValueError("Invalid actor path")

    system, separator, endpoint = authority.partition("@")
    if not _is_remote_name(system):
        raise ValueError("Invalid actor system name")
    host = None
    port = None
    if separator:
        host, port = _parse_endpoint(endpoint)

    if not remote_path:
        return RootActorPath(Address(protocol, system, host, port))

    elements = remote_path.split("/")
    if any(not _is_remote_name(element) for element in elements):
        raise ValueError("Actor path contains a non-resolvable element")

    path: ActorPath = RootActorPath(Address(protocol, system, host, port))
    for element in elements:
        path = path.child(element)
    if str(path) != value:
        raise ValueError("Actor path is not canonical")
    return path


def is_remote_actor_path(value: str) -> bool:
    if not isinstance(value, str) or not value.startswith("/") or value.startswith("//"):
        return False
    elements = value[1:].split("/")
    return bool(elements) and all(_is_remote_name(element) for element in elements)


def _is_remote_name(value: str) -> bool:
    return isinstance(value, str) and value.isascii() and _REMOTE_NAME.fullmatch(value) is not None


def _parse_endpoint(endpoint: str) -> tuple[str, int | None]:
    if not endpoint or not endpoint.isascii():
        raise ValueError("Invalid actor path endpoint")
    if endpoint.startswith("["):
        closing = endpoint.find("]")
        if closing == -1:
            raise ValueError("Invalid actor path endpoint")
        host = endpoint[1:closing]
        try:
            canonical_host = str(ipaddress.IPv6Address(host))
        except ValueError as error:
            raise ValueError("Invalid actor path endpoint") from error
        if canonical_host != host:
            raise ValueError("Actor path IPv6 host is not canonical")
        remaining = endpoint[closing + 1 :]
        if not remaining:
            return host, None
        if not remaining.startswith(":"):
            raise ValueError("Invalid actor path endpoint")
        return host, _parse_port(remaining[1:])
    if endpoint.count(":") > 1:
        raise ValueError("IPv6 hosts must use brackets")
    host, separator, port_text = endpoint.partition(":")
    if _HOST.fullmatch(host) is None:
        raise ValueError("Invalid actor path endpoint")
    return host, _parse_port(port_text) if separator else None


def _parse_port(value: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise ValueError("Invalid actor path port")
    port = int(value)
    if not 1 <= port <= 65_535:
        raise ValueError("Actor path port must be between 1 and 65535")
    return port
