from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class Address:
    protocol: str
    system: str
    host: Optional[str] = None
    port: Optional[int] = None

    @property
    def has_local_scope(self) -> bool:
        return self.host is None

    @property
    def has_global_scope(self) -> bool:
        return self.host is not None

    def __str__(self) -> str:
        addr = f"{self.protocol}://{self.system}"
        if self.host:
            addr += f"@{self.host}"
            if self.port:
                addr += f":{self.port}"
        return addr


def split_name_and_uid(name: str) -> tuple[str, Optional[int]]:
    if "#" in name:
        parts = name.rsplit("@", 1)
        return parts[0], int(parts[1])
    else:
        return name, None


class ActorPath(Protocol):
    _address: Address
    _name: str
    _parent: Optional["ActorPath"]
    _uid: Optional[int]

    def __init__(
        self,
        address: Address,
        name: str,
        parent: Optional["ActorPath"] = None,
        uid: Optional[int] = None,
    ) -> None:
        self._address = address
        self._name = name
        self._parent = parent
        self._uid = uid

    def child(self, child: str) -> "ActorPath": ...

    def __truediv__(self, child: str) -> "ActorPath":
        return self.child(child)

    def elements(self) -> list[str]: ...


class RootActorPath(ActorPath):
    def __init__(self, address: Address) -> None:
        super().__init__(address, name="/", parent=None)
        self._parent = self

    def child(self, child: str) -> ActorPath:
        (name, uid) = split_name_and_uid(child)
        return ChildActorPath(
            name=name,
            parent=self,
            uid=uid,
        )

    def elements(self) -> list[str]:
        return ["/"]


class ChildActorPath(ActorPath):
    def __init__(self, name: str, parent: ActorPath, uid: Optional[int] = None) -> None:
        if "#" in name:
            raise ValueError("Name cannot contain '#' character")
        if "/" in name:
            raise ValueError("Name cannot contain '/' character")
        super().__init__(address=parent._address, name=name, parent=parent, uid=uid)

    def child(self, child: str) -> ActorPath:
        (name, uid) = split_name_and_uid(child)
        return ChildActorPath(
            name=name,
            parent=self,
            uid=uid,
        )

    def elements(self) -> list[str]:
        elems = []
        current: Optional[ActorPath] = self
        while current is not None and current._parent != current:
            elems.append(current._name)
            current = current._parent
        elems.append("/")
        elems.reverse()
        return elems
