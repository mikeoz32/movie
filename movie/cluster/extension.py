"""Actor System extension entry point for cluster membership."""

from __future__ import annotations

from movie.actor.extension import ExtensionId
from movie.cluster.runtime import ClusterRuntime
from movie.remoting.extension import REMOTING


class ClusterExtension(ClusterRuntime):
    """Cluster membership owned by an Actor System extension registry."""

    def start(self) -> None:
        if self._system._is_in_actor_callback():
            raise RuntimeError("cluster extension cannot start from an actor callback")
        if REMOTING.get(self._system) is not self._remoting:
            raise RuntimeError("cluster remoting dependency changed during startup")
        super().start()

    def prepare_stop(self, timeout: float) -> None:
        self.leave(timeout)

    def stop(self, timeout: float) -> None:
        self.leave(timeout)


def _create_cluster_extension(system) -> ClusterExtension:
    return system._create_cluster_extension()


CLUSTER: ExtensionId[ClusterExtension] = ExtensionId(
    "cluster",
    _create_cluster_extension,
)


__all__ = ["CLUSTER", "ClusterExtension"]
