import sqlite3
import threading
import time

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.io import ASYNCIO_IO
from movie.persistence import (
    DURABLE_STATE,
    ConcurrentWriteError,
    DurableStateRecord,
    OperationConflictError,
    OperationId,
    PersistenceCapacityError,
    PersistenceId,
    PersistenceOperationTimeout,
    PersistenceSchemaError,
    WriteResult,
    persistence_slice,
)


def create_system(
    name: str,
    path: str,
    persistence: dict | None = None,
) -> ActorSystem:
    persistence_settings = {"sqlite": {"path": path}}
    persistence_settings.update(persistence or {})
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=Config(
            {
                "movie": {
                    "persistence": persistence_settings
                }
            }
        ),
    )


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def test_persistence_slice_has_stable_golden_vectors() -> None:
    assert persistence_slice(PersistenceId("counter", "one")) == 230
    assert persistence_slice(PersistenceId("order", "A-42")) == 888


def test_sqlite_store_recovers_committed_state_in_a_new_actor_system(tmp_path) -> None:
    path = str(tmp_path / "durable-state.sqlite3")
    persistence_id = PersistenceId("counter", "one")
    operation_id = OperationId.random()
    first = create_system("durable-state-first", path)
    try:
        store = DURABLE_STATE.get(first).store
        assert store.load(persistence_id).result(1.0) is None

        result = store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=operation_id,
            manifest="counter-state/v1",
            payload=b'{"value":1}',
        ).result(1.0)

        assert result.revision == 1
        assert not result.duplicate
    finally:
        first.stop()

    second = create_system("durable-state-second", path)
    try:
        recovered = DURABLE_STATE.get(second).store.load(persistence_id).result(1.0)

        assert recovered is not None
        assert recovered.persistence_id == persistence_id
        assert recovered.revision == 1
        assert recovered.operation_id == operation_id
        assert recovered.manifest == "counter-state/v1"
        assert recovered.payload == b'{"value":1}'
        assert not recovered.deleted
    finally:
        second.stop()


def test_store_deduplicates_operations_and_rejects_conflicting_writes(tmp_path) -> None:
    system = create_system("durable-state-conflicts", str(tmp_path / "conflicts.sqlite3"))
    persistence_id = PersistenceId("counter", "conflicts")
    operation_id = OperationId.random()
    try:
        store = DURABLE_STATE.get(system).store
        first = store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=operation_id,
            manifest="counter-state/v1",
            payload=b'{"value":1}',
        ).result(1.0)
        duplicate = store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=operation_id,
            manifest="counter-state/v1",
            payload=b'{"value":1}',
        ).result(1.0)

        assert first.revision == duplicate.revision == 1
        assert duplicate.duplicate

        with pytest.raises(OperationConflictError):
            store.upsert(
                persistence_id,
                expected_revision=1,
                operation_id=operation_id,
                manifest="counter-state/v1",
                payload=b'{"value":2}',
            ).result(1.0)

        with pytest.raises(ConcurrentWriteError) as failure:
            store.upsert(
                persistence_id,
                expected_revision=0,
                operation_id=OperationId.random(),
                manifest="counter-state/v1",
                payload=b'{"value":2}',
            ).result(1.0)

        assert failure.value.expected_revision == 0
        assert failure.value.actual_revision == 1
    finally:
        system.stop()


def test_change_feed_retains_each_nonduplicate_state_revision(tmp_path) -> None:
    system = create_system("durable-state-changes", str(tmp_path / "changes.sqlite3"))
    persistence_id = PersistenceId("counter", "changes")
    first_operation = OperationId.random()
    second_operation = OperationId.random()
    delete_operation = OperationId.random()
    try:
        store = DURABLE_STATE.get(system).store
        store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=first_operation,
            manifest="counter/v1",
            payload=b'{"value":1}',
        ).result(1.0)
        store.upsert(
            persistence_id,
            expected_revision=1,
            operation_id=second_operation,
            manifest="counter/v1",
            payload=b'{"value":2}',
        ).result(1.0)
        duplicate = store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=first_operation,
            manifest="counter/v1",
            payload=b'{"value":1}',
        ).result(1.0)
        store.delete(
            persistence_id,
            expected_revision=2,
            operation_id=delete_operation,
        ).result(1.0)

        batch = store.changes(
            "counter",
            min_slice=0,
            max_slice=1023,
            after_offset=0,
            limit=10,
        ).result(1.0)

        assert duplicate.duplicate
        assert [change.revision for change in batch.changes] == [1, 2, 3]
        assert [change.operation_id for change in batch.changes] == [
            first_operation,
            second_operation,
            delete_operation,
        ]
        assert [change.payload for change in batch.changes] == [
            b'{"value":1}',
            b'{"value":2}',
            None,
        ]
        assert [change.deleted for change in batch.changes] == [False, False, True]
        assert batch.offset == batch.changes[-1].offset

        resumed = store.changes(
            "counter",
            min_slice=0,
            max_slice=1023,
            after_offset=batch.changes[0].offset,
            limit=10,
        ).result(1.0)
        assert [change.revision for change in resumed.changes] == [2, 3]
    finally:
        system.stop()


def test_change_feed_advances_across_changes_outside_its_source(tmp_path) -> None:
    system = create_system(
        "durable-state-change-watermark",
        str(tmp_path / "change-watermark.sqlite3"),
    )
    try:
        store = DURABLE_STATE.get(system).store
        store.upsert(
            PersistenceId("order", "one"),
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="order/v1",
            payload=b"first",
        ).result(1.0)

        empty = store.changes(
            "counter",
            min_slice=0,
            max_slice=1023,
            after_offset=0,
            limit=10,
        ).result(1.0)

        assert empty.changes == ()
        assert empty.offset == 1

        store.upsert(
            PersistenceId("counter", "one"),
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="counter/v1",
            payload=b"second",
        ).result(1.0)
        resumed = store.changes(
            "counter",
            min_slice=0,
            max_slice=1023,
            after_offset=empty.offset,
            limit=10,
        ).result(1.0)
        assert [change.payload for change in resumed.changes] == [b"second"]
        assert resumed.changes[0].offset == 2
    finally:
        system.stop()


def test_delete_writes_a_revisioned_tombstone(tmp_path) -> None:
    system = create_system("durable-state-delete", str(tmp_path / "delete.sqlite3"))
    persistence_id = PersistenceId("profile", "deleted")
    delete_id = OperationId.random()
    try:
        store = DURABLE_STATE.get(system).store
        deleted = store.delete(
            persistence_id,
            expected_revision=0,
            operation_id=delete_id,
        ).result(1.0)
        duplicate = store.delete(
            persistence_id,
            expected_revision=0,
            operation_id=delete_id,
        ).result(1.0)

        assert deleted.revision == duplicate.revision == 1
        assert duplicate.duplicate
        tombstone = store.load(persistence_id).result(1.0)
        assert tombstone is not None
        assert tombstone.revision == 1
        assert tombstone.operation_id == delete_id
        assert tombstone.manifest is None
        assert tombstone.payload is None
        assert tombstone.deleted

        restored = store.upsert(
            persistence_id,
            expected_revision=1,
            operation_id=OperationId.random(),
            manifest="profile/v1",
            payload=b'{"name":"Ada"}',
        ).result(1.0)

        assert restored.revision == 2
        assert not store.load(persistence_id).result(1.0).deleted
    finally:
        system.stop()


def test_store_rejects_operations_beyond_its_own_capacity(tmp_path) -> None:
    path = str(tmp_path / "capacity.sqlite3")
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "durable-state-capacity",
        config=Config(
            {
                "movie": {
                    "persistence": {
                        "operation-capacity": 1,
                        "pending-byte-capacity": 1_024,
                        "max-state-bytes": 1_024,
                        "sqlite": {"path": path},
                    }
                }
            }
        ),
    )
    locker = None
    try:
        store = DURABLE_STATE.get(system).store
        locker = sqlite3.connect(path, isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        pending = store.upsert(
            PersistenceId("capacity", "one"),
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="capacity/v1",
            payload=b"one",
        )
        wait_until(lambda: store.pending_operations == 1)

        with pytest.raises(PersistenceCapacityError):
            store.load(PersistenceId("capacity", "two"))

        locker.rollback()
        assert pending.result(1.0).revision == 1
        assert store.pending_operations == 0
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        system.stop()


def test_actor_system_shutdown_can_resume_after_an_accepted_write(tmp_path) -> None:
    path = str(tmp_path / "shutdown.sqlite3")
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "durable-state-shutdown",
        config=Config(
            {
                "movie": {
                    "io": {"asyncio": {"command-capacity": 1}},
                    "persistence": {
                        "operation-capacity": 1,
                        "sqlite": {"path": path},
                    },
                }
            }
        ),
    )
    locker = None
    stopped = False
    try:
        store = DURABLE_STATE.get(system).store
        io = ASYNCIO_IO.get(system)
        locker = sqlite3.connect(path, isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        pending = store.upsert(
            PersistenceId("shutdown", "one"),
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="shutdown/v1",
            payload=b"accepted",
        )
        wait_until(lambda: store.executing_operations == 1)
        with pytest.raises(TimeoutError):
            system.stop(0.05)
        locker.rollback()
        locker.close()
        locker = None

        assert pending.result(1.0).revision == 1
        system.stop(1.0)
        stopped = True

        assert not io.thread.is_alive()
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        if not stopped:
            system.stop(1.0)


def test_store_rejects_a_newer_sqlite_schema(tmp_path) -> None:
    path = str(tmp_path / "newer-schema.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA user_version=2")
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="newer schema version 2"):
        create_system("durable-state-newer-schema", path)


def test_store_startup_timeout_bounds_an_external_database_lock(tmp_path) -> None:
    path = str(tmp_path / "startup-timeout.sqlite3")
    locker = sqlite3.connect(path, isolation_level=None)
    locker.execute("BEGIN EXCLUSIVE")
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="did not start in time"):
            create_system(
                "durable-state-startup-timeout",
                path,
                persistence={"startup-timeout": 0.05},
            )
        assert time.monotonic() - started < 2.0
    finally:
        locker.rollback()
        locker.close()


def test_store_rejects_an_incompatible_versioned_schema(tmp_path) -> None:
    path = str(tmp_path / "incompatible-schema.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE movie_durable_state (wrong TEXT)")
        connection.execute("PRAGMA user_version=1")
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="incompatible"):
        create_system("durable-state-incompatible-schema", path)


def test_store_rejects_an_incomplete_versioned_schema(tmp_path) -> None:
    path = str(tmp_path / "incomplete-schema.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA user_version=1")
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="incompatible"):
        create_system("durable-state-incomplete-schema", path)


def test_store_rejects_a_versioned_schema_without_the_change_query_index(
    tmp_path,
) -> None:
    path = str(tmp_path / "missing-change-index.sqlite3")
    system = create_system("durable-state-index-writer", path)
    system.stop()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP INDEX movie_durable_change_entity_offset")
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="query index"):
        create_system("durable-state-index-reader", path)


def test_store_rejects_an_extra_unique_change_feed_index(tmp_path) -> None:
    path = str(tmp_path / "unique-change-index.sqlite3")
    system = create_system("durable-state-unique-index-writer", path)
    system.stop()
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE UNIQUE INDEX invalid_change_index "
            "ON movie_durable_change (entity_type)"
        )
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="incompatible indexes"):
        create_system("durable-state-unique-index-reader", path)


def test_store_rejects_case_insensitive_persistence_identity_keys(tmp_path) -> None:
    path = str(tmp_path / "nocase-schema.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE movie_durable_state (
                entity_type TEXT COLLATE NOCASE NOT NULL,
                entity_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                operation_id TEXT NOT NULL,
                manifest TEXT,
                payload BLOB,
                deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
                CHECK (
                    (deleted = 0 AND manifest IS NOT NULL AND payload IS NOT NULL)
                    OR (deleted = 1 AND manifest IS NULL AND payload IS NULL)
                ),
                PRIMARY KEY (entity_type, entity_id)
            );
            CREATE TABLE movie_durable_operation (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                fingerprint BLOB NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                PRIMARY KEY (entity_type, entity_id, operation_id)
            );
            PRAGMA user_version=1;
            """
        )
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="incompatible"):
        create_system("durable-state-nocase-schema", path)


def test_store_rejects_a_non_utf8_database(tmp_path) -> None:
    path = str(tmp_path / "utf16-schema.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA encoding='UTF-16'")
        connection.execute("CREATE TABLE marker (value TEXT)")
    finally:
        connection.close()

    with pytest.raises(PersistenceSchemaError, match="UTF-8"):
        create_system("durable-state-utf16-schema", path)


def test_load_rejects_state_larger_than_the_current_limit(tmp_path) -> None:
    path = str(tmp_path / "recovery-capacity.sqlite3")
    persistence_id = PersistenceId("capacity", "recovery")
    first = create_system("durable-state-large-writer", path)
    try:
        DURABLE_STATE.get(first).store.upsert(
            persistence_id,
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="capacity/v1",
            payload=b"large-state",
        ).result(1.0)
    finally:
        first.stop()

    second = create_system(
        "durable-state-small-reader",
        path,
        persistence={"max-state-bytes": 8},
    )
    try:
        with pytest.raises(PersistenceCapacityError, match="max-state-bytes"):
            DURABLE_STATE.get(second).store.load(persistence_id).result(1.0)
    finally:
        second.stop()


@pytest.mark.parametrize(
    "path",
    [":memory:", "file::memory:?cache=shared", "file:state?mode=memory&cache=shared"],
)
def test_durable_state_rejects_an_in_memory_sqlite_database(path) -> None:
    with pytest.raises(ValueError, match="file-backed"):
        create_system("durable-state-memory", path)


def test_mutation_times_out_before_a_locked_transaction_starts(tmp_path) -> None:
    path = str(tmp_path / "writer-timeout.sqlite3")
    system = create_system(
        "durable-state-writer-timeout",
        path,
        persistence={"operation-timeout": 0.05},
    )
    locker = sqlite3.connect(path, isolation_level=None)
    try:
        store = DURABLE_STATE.get(system).store
        persistence_id = PersistenceId("timeout", "writer")
        locker.execute("BEGIN EXCLUSIVE")

        with pytest.raises(PersistenceOperationTimeout):
            store.upsert(
                persistence_id,
                expected_revision=0,
                operation_id=OperationId.random(),
                manifest="timeout/v1",
                payload=b"not-committed",
            ).result(0.5)

        locker.rollback()
        assert (
            store.upsert(
                persistence_id,
                expected_revision=0,
                operation_id=OperationId.random(),
                manifest="timeout/v1",
                payload=b"committed",
            ).result(1.0).revision
            == 1
        )
    finally:
        locker.close()
        system.stop()


def test_durable_state_records_enforce_live_and_tombstone_invariants() -> None:
    persistence_id = PersistenceId("model", "one")
    operation_id = OperationId.random()

    with pytest.raises(ValueError, match="positive"):
        WriteResult(0)
    with pytest.raises(ValueError, match="manifest and payload"):
        DurableStateRecord(
            persistence_id,
            1,
            operation_id,
            None,
            None,
            deleted=False,
        )
    with pytest.raises(ValueError, match="must not contain"):
        DurableStateRecord(
            persistence_id,
            1,
            operation_id,
            "model/v1",
            b"state",
            deleted=True,
        )


def test_load_timeout_does_not_cancel_an_accepted_write(tmp_path) -> None:
    path = str(tmp_path / "operation-timeout.sqlite3")
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "durable-state-operation-timeout",
        config=Config(
            {
                "movie": {
                    "persistence": {
                        "operation-capacity": 2,
                        "operation-timeout": 1.0,
                        "recovery-timeout": 0.05,
                        "sqlite": {"path": path},
                    }
                }
            }
        ),
    )
    locker = None
    try:
        store = DURABLE_STATE.get(system).store
        locker = sqlite3.connect(path, isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        pending = store.upsert(
            PersistenceId("timeout", "write"),
            expected_revision=0,
            operation_id=OperationId.random(),
            manifest="timeout/v1",
            payload=b"accepted",
        )
        wait_until(lambda: store.executing_operations == 1)
        with pytest.raises(PersistenceOperationTimeout):
            store.load(PersistenceId("timeout", "load")).result(0.5)
        locker.rollback()
        locker.close()
        locker = None

        assert pending.result(1.0).revision == 1
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        system.stop()


def test_store_maps_shared_asyncio_capacity_to_persistence_error(tmp_path) -> None:
    path = str(tmp_path / "shared-capacity.sqlite3")
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "durable-state-shared-capacity",
        config=Config(
            {
                "movie": {
                    "io": {"asyncio": {"command-capacity": 1}},
                    "persistence": {"sqlite": {"path": path}},
                }
            }
        ),
    )
    io = ASYNCIO_IO.get(system)
    store = DURABLE_STATE.get(system).store
    entered = threading.Event()
    release = threading.Event()

    def block_io() -> None:
        entered.set()
        release.wait(1.0)

    try:
        blocked = io.schedule(block_io)
        assert entered.wait(1.0)
        with pytest.raises(PersistenceCapacityError, match="Shared Asyncio"):
            store.load(PersistenceId("capacity", "shared"))
        release.set()
        blocked.result(1.0)
    finally:
        release.set()
        system.stop()


@pytest.mark.parametrize("setting", ["operation-timeout", "recovery-timeout"])
@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf")])
def test_persistence_rejects_nonfinite_timeout(tmp_path, setting, timeout) -> None:
    with pytest.raises(ValueError, match="positive number"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "durable-state-invalid-timeout",
            config=Config(
                {
                    "movie": {
                        "persistence": {
                            setting: timeout,
                            "sqlite": {"path": str(tmp_path / "invalid.sqlite3")},
                        }
                    }
                }
            ),
        )
