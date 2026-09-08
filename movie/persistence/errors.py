class PersistenceError(RuntimeError):
    pass


class PersistenceCapacityError(PersistenceError):
    pass


class PersistenceSchemaError(PersistenceError):
    pass


class ChangeFeedCompactedError(PersistenceError):
    def __init__(self, requested_offset: int, compacted_through: int) -> None:
        super().__init__(
            f"Projection Offset {requested_offset} precedes compacted Change Feed "
            f"offset {compacted_through}"
        )
        self.requested_offset = requested_offset
        self.compacted_through = compacted_through


class PersistenceOperationTimeout(PersistenceError, TimeoutError):
    pass


class ConcurrentWriteError(PersistenceError):
    def __init__(self, expected_revision: int, actual_revision: int) -> None:
        super().__init__(
            f"Expected durable-state revision {expected_revision}, found {actual_revision}"
        )
        self.expected_revision = expected_revision
        self.actual_revision = actual_revision


class OperationConflictError(PersistenceError):
    pass


class DurableStateRecoveryError(PersistenceError):
    pass


class DurableStatePersistError(PersistenceError):
    pass


__all__ = [
    "ConcurrentWriteError",
    "ChangeFeedCompactedError",
    "DurableStatePersistError",
    "DurableStateRecoveryError",
    "OperationConflictError",
    "PersistenceCapacityError",
    "PersistenceError",
    "PersistenceOperationTimeout",
    "PersistenceSchemaError",
]
