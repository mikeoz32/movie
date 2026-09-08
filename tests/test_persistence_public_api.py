import movie.persistence as persistence_api


def test_persistence_package_exports_only_public_api() -> None:
    assert set(persistence_api.__all__) == {
        "DURABLE_STATE",
        "ChangeFeedCompactedError",
        "ConcurrentWriteError",
        "DurableEffect",
        "DurableStateBehavior",
        "DurableStateChange",
        "DurableStateChangeBatch",
        "DurableStateExtension",
        "DurableStatePersistError",
        "DurableStateRecord",
        "DurableStateRecoveryError",
        "DurableStateStore",
        "EncodedState",
        "OperationConflictError",
        "OperationId",
        "PERSISTENCE_SLICE_COUNT",
        "PersistenceCapacityError",
        "PersistenceError",
        "PersistenceId",
        "PersistenceOperationTimeout",
        "PersistenceSchemaError",
        "SQLiteDurableStateStore",
        "StateCodec",
        "WriteResult",
        "persistence_slice",
    }
