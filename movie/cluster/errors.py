"""Public cluster runtime errors."""


class ClusterError(Exception):
    """Base class for cluster failures."""


class ClusterJoinError(ClusterError):
    """An actor system incarnation could not join its configured cluster."""


class ClusterShutdownError(ClusterError):
    """A cluster runtime could not stop before its deadline."""


__all__ = ["ClusterError", "ClusterJoinError", "ClusterShutdownError"]
