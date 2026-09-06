"""Actor System extension entry point for remoting."""

from __future__ import annotations

from movie.actor.extension import ExtensionId
from movie.remoting.runtime import RemotingRuntime


class RemotingExtension(RemotingRuntime):
    """The remoting runtime owned by an Actor System extension registry."""

    def prepare_stop(self, timeout: float) -> None:
        with self._condition:
            starting = self._state.value in ("new", "starting")
        if starting:
            self.stop(timeout)


def _create_remoting_extension(system) -> RemotingExtension:
    return system._create_remoting_extension()


REMOTING: ExtensionId[RemotingExtension] = ExtensionId(
    "remoting",
    _create_remoting_extension,
)


__all__ = ["REMOTING", "RemotingExtension"]
