class ProjectionError(RuntimeError):
    pass


class ProjectionHandlerTimeout(ProjectionError, TimeoutError):
    pass


class ProjectionCapacityError(ProjectionError):
    pass


class ProjectionBaselineError(ProjectionError):
    pass


class ProjectionAlreadyRunningError(ProjectionError):
    pass


class ProjectionSourceConflictError(ProjectionError):
    pass


class ProjectionTransactionError(ProjectionError):
    pass


class ProjectionOffsetConflictError(ProjectionError):
    pass


__all__ = [
    "ProjectionCapacityError",
    "ProjectionBaselineError",
    "ProjectionAlreadyRunningError",
    "ProjectionError",
    "ProjectionHandlerTimeout",
    "ProjectionOffsetConflictError",
    "ProjectionSourceConflictError",
    "ProjectionTransactionError",
]
