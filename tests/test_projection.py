import asyncio
import sqlite3
import time
from queue import Queue
from threading import Event

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.persistence import (
    DURABLE_STATE,
    ChangeFeedCompactedError,
    OperationId,
    PersistenceCapacityError,
    PersistenceId,
    persistence_slice,
)
from movie.projection import (
    PROJECTIONS,
    ProjectionBaselineError,
    ProjectionId,
    ProjectionSourceConflictError,
    ProjectionTransactionError,
)


def create_system(
    name: str,
    path: str,
    *,
    asyncio_capacity: int | None = None,
    persistence: dict | None = None,
    projection: dict | None = None,
) -> ActorSystem:
    persistence_config = {"sqlite": {"path": path}}
    persistence_config.update(persistence or {})
    projection_config = {
        "poll-interval": 0.01,
        "retry-min-backoff": 0.01,
        "retry-max-backoff": 0.05,
    }
    projection_config.update(projection or {})
    movie_config = {
        "persistence": persistence_config,
        "projection": projection_config,
    }
    if asyncio_capacity is not None:
        movie_config["io"] = {"asyncio": {"command-capacity": asyncio_capacity}}
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=Config(
            {
                "movie": movie_config
            }
        ),
    )


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition was not met before the deadline")
        time.sleep(0.005)


def persist(store, persistence_id: PersistenceId, revision: int, payload: bytes) -> None:
    store.upsert(
        persistence_id,
        expected_revision=revision - 1,
        operation_id=OperationId.random(),
        manifest="counter/v1",
        payload=payload,
    ).result(1.0)


def test_at_least_once_projection_resumes_from_its_stored_offset(tmp_path) -> None:
    system = create_system("projection-resume", str(tmp_path / "projection.sqlite3"))
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "one")
    projection_id = ProjectionId("counter-view", "all")
    observed = Queue()

    async def first_handler(changes) -> None:
        observed.put_nowait(tuple(change.revision for change in changes))

    for revision in range(1, 4):
        persist(store, persistence_id, revision, str(revision).encode("ascii"))

    first = PROJECTIONS.get(system).run_at_least_once(
        projection_id,
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=first_handler,
        batch_size=2,
    )
    try:
        assert first.wait_started(1.0)
        assert observed.get(timeout=1.0) == (1, 2)
        assert observed.get(timeout=1.0) == (3,)
        wait_until(lambda: first.offset == 3)
        first.stop(1.0)

        resumed_change = Event()

        async def resumed_handler(changes) -> None:
            observed.put_nowait(tuple(change.revision for change in changes))
            resumed_change.set()

        second = PROJECTIONS.get(system).run_at_least_once(
            projection_id,
            entity_type="counter",
            min_slice=0,
            max_slice=1023,
            handler=resumed_handler,
            batch_size=2,
        )
        assert second.wait_started(1.0)
        assert second.offset == 3
        assert not resumed_change.wait(0.05)

        persist(store, persistence_id, 4, b"4")
        assert resumed_change.wait(1.0)
        assert observed.get(timeout=1.0) == (4,)
        wait_until(lambda: second.offset == 4)
        second.stop(1.0)
    finally:
        system.stop()


def test_projection_checkpoint_survives_actor_system_restart(tmp_path) -> None:
    path = str(tmp_path / "projection-system-restart.sqlite3")
    projection_id = ProjectionId("counter-view", "system-restart")
    persistence_id = PersistenceId("counter", "system-restart")
    first_system = create_system("projection-system-restart-first", path)
    first_seen = Event()

    async def first_handler(changes) -> None:
        first_seen.set()

    try:
        persist(DURABLE_STATE.get(first_system).store, persistence_id, 1, b"1")
        first = PROJECTIONS.get(first_system).run_at_least_once(
            projection_id,
            entity_type="counter",
            min_slice=0,
            max_slice=1023,
            handler=first_handler,
        )
        assert first_seen.wait(1.0)
        wait_until(lambda: first.offset == 1)
    finally:
        first_system.stop()

    second_system = create_system("projection-system-restart-second", path)
    second_seen = Event()

    async def second_handler(changes) -> None:
        second_seen.set()

    second = PROJECTIONS.get(second_system).run_at_least_once(
        projection_id,
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=second_handler,
    )
    try:
        assert second.wait_started(1.0)
        assert second.offset == 1
        assert not second_seen.wait(0.05)
        persist(DURABLE_STATE.get(second_system).store, persistence_id, 2, b"2")
        assert second_seen.wait(1.0)
        wait_until(lambda: second.offset == 2)
    finally:
        second_system.stop()


def test_at_least_once_projection_retries_a_failed_batch(tmp_path) -> None:
    system = create_system("projection-retry", str(tmp_path / "projection-retry.sqlite3"))
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "retry")
    persist(store, persistence_id, 1, b"1")
    attempts = Queue()
    completed = Event()

    async def handler(changes) -> None:
        attempts.put_nowait(tuple(change.revision for change in changes))
        if attempts.qsize() == 1:
            raise RuntimeError("retry this batch")
        completed.set()

    projection = PROJECTIONS.get(system).run_at_least_once(
        ProjectionId("counter-view", "retry"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert projection.wait_started(1.0)
        assert completed.wait(1.0), projection.last_error
        wait_until(lambda: projection.offset == 1)
        assert attempts.get_nowait() == (1,)
        assert attempts.get_nowait() == (1,)
        assert projection.failure is None
        assert isinstance(projection.last_error, RuntimeError)
    finally:
        projection.stop(1.0)
        system.stop()


def test_idle_projection_does_not_consume_shared_asyncio_command_capacity(
    tmp_path,
) -> None:
    system = create_system(
        "projection-asyncio-capacity",
        str(tmp_path / "projection-asyncio-capacity.sqlite3"),
        asyncio_capacity=1,
    )

    async def handler(changes) -> None:
        pass

    projection = PROJECTIONS.get(system).run_at_least_once(
        ProjectionId("counter-view", "asyncio-capacity"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert projection.wait_started(1.0)
        persist(
            DURABLE_STATE.get(system).store,
            PersistenceId("counter", "asyncio-capacity"),
            1,
            b"1",
        )
    finally:
        projection.stop(1.0)
        system.stop()


def test_exactly_once_projection_commits_read_model_and_offset_together(tmp_path) -> None:
    path = str(tmp_path / "projection-exactly-once.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE counter_projection_effect (
                effect_id INTEGER PRIMARY KEY AUTOINCREMENT,
                revision INTEGER NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    system = create_system("projection-exactly-once", path)
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "exactly-once")
    persist(store, persistence_id, 1, b"1")
    persist(store, persistence_id, 2, b"2")
    attempts = 0
    completed = Event()

    async def handler(transaction, changes) -> None:
        nonlocal attempts
        attempts += 1
        for change in changes:
            await transaction.execute(
                "INSERT INTO counter_projection_effect (revision) VALUES (?)",
                (change.revision,),
            )
        if attempts == 1:
            raise RuntimeError("rollback this projection transaction")
        completed.set()

    projection = PROJECTIONS.get(system).run_exactly_once(
        ProjectionId("counter-view", "exactly-once"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert projection.wait_started(1.0)
        assert completed.wait(1.0)
        wait_until(lambda: projection.offset == 2)

        connection = sqlite3.connect(path)
        try:
            rows = connection.execute(
                "SELECT revision FROM counter_projection_effect ORDER BY effect_id"
            ).fetchall()
        finally:
            connection.close()

        assert attempts == 2
        assert rows == [(1,), (2,)]
    finally:
        projection.stop(1.0)
        system.stop()


def test_exactly_once_handler_cannot_escape_its_transaction(tmp_path) -> None:
    path = str(tmp_path / "projection-transaction-escape.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE projection_effect (value INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()

    system = create_system("projection-transaction-escape", path)
    store = DURABLE_STATE.get(system).store
    persist(store, PersistenceId("counter", "escape"), 1, b"1")

    async def handler(transaction, changes) -> None:
        await transaction.execute("INSERT INTO projection_effect VALUES (1)")
        await transaction.execute("-- transaction-control bypass\nCOMMIT")

    projection = PROJECTIONS.get(system).run_exactly_once(
        ProjectionId("counter-view", "escape"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert projection.wait_stopped(1.0)
        assert isinstance(projection.failure, ProjectionTransactionError)
        connection = sqlite3.connect(path)
        try:
            count = connection.execute("SELECT count(*) FROM projection_effect").fetchone()[0]
        finally:
            connection.close()
        assert count == 0
    finally:
        system.stop()


def test_exactly_once_handler_cannot_hide_a_forbidden_sqlite_write(tmp_path) -> None:
    path = str(tmp_path / "projection-sqlite-metadata.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE projection_effect (value INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()
    system = create_system("projection-sqlite-metadata", path)
    store = DURABLE_STATE.get(system).store
    persist(store, PersistenceId("counter", "sqlite-metadata"), 1, b"1")

    async def handler(transaction, changes) -> None:
        await transaction.execute("INSERT INTO projection_effect VALUES (1)")
        try:
            await transaction.execute(
                "UPDATE sqlite_sequence SET seq = 9223372036854775807 "
                "WHERE name = 'movie_durable_change'"
            )
        except sqlite3.DatabaseError:
            pass

    projection = PROJECTIONS.get(system).run_exactly_once(
        ProjectionId("counter-view", "sqlite-metadata"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert projection.wait_stopped(1.0)
        assert isinstance(projection.failure, ProjectionTransactionError)
        connection = sqlite3.connect(path)
        try:
            count = connection.execute("SELECT count(*) FROM projection_effect").fetchone()[0]
        finally:
            connection.close()
        assert count == 0
    finally:
        system.stop()


def test_change_feed_compaction_stops_at_the_slowest_registered_projection(
    tmp_path,
) -> None:
    system = create_system("projection-compaction", str(tmp_path / "compaction.sqlite3"))
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "compaction")
    projections = PROJECTIONS.get(system)
    for revision in range(1, 4):
        persist(store, persistence_id, revision, str(revision).encode("ascii"))

    async def fast_handler(changes) -> None:
        pass

    fast = projections.run_at_least_once(
        ProjectionId("counter-view", "fast"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=fast_handler,
    )
    slow_entered = Event()
    release_slow = Event()

    async def slow_handler(changes) -> None:
        if changes[0].revision > 1:
            slow_entered.set()
            while not release_slow.is_set():
                await asyncio.sleep(0.005)

    slow = projections.run_at_least_once(
        ProjectionId("counter-view", "slow"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=slow_handler,
        batch_size=1,
    )
    try:
        assert fast.wait_started(1.0)
        assert slow.wait_started(1.0)
        wait_until(lambda: fast.offset == 3)
        assert slow_entered.wait(1.0)
        assert slow.offset == 1

        assert projections.compact_changes().result(1.0) == 1

        retained = store.changes(
            "counter",
            min_slice=0,
            max_slice=1023,
            after_offset=1,
            limit=10,
        ).result(1.0)
        assert [change.revision for change in retained.changes] == [2, 3]

        release_slow.set()
        slow.stop(1.0)
        assert projections.retire(slow.projection_id).result(1.0)
        assert projections.compact_changes().result(1.0) == 2
    finally:
        release_slow.set()
        fast.stop(1.0)
        if slow.is_running:
            slow.stop(1.0)
        system.stop()


def test_projection_requires_an_explicit_baseline_after_compaction(tmp_path) -> None:
    system = create_system("projection-baseline", str(tmp_path / "baseline.sqlite3"))
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "baseline")
    projections = PROJECTIONS.get(system)
    persist(store, persistence_id, 1, b"1")

    processed = Event()

    async def handler(changes) -> None:
        processed.set()

    original = projections.run_at_least_once(
        ProjectionId("counter-view", "original"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert processed.wait(1.0)
        wait_until(lambda: original.offset == 1)
        original.stop(1.0)
        assert projections.compact_changes().result(1.0) == 1
        with pytest.raises(ChangeFeedCompactedError):
            store.changes(
                "counter",
                min_slice=0,
                max_slice=1023,
                after_offset=0,
                limit=10,
            ).result(1.0)

        missing_baseline = projections.run_at_least_once(
            ProjectionId("counter-view", "missing-baseline"),
            entity_type="counter",
            min_slice=0,
            max_slice=1023,
            handler=handler,
        )
        assert missing_baseline.wait_stopped(1.0)
        assert isinstance(missing_baseline.failure, ProjectionBaselineError)

        resumed = Event()

        async def resumed_handler(changes) -> None:
            resumed.set()

        baseline = projections.run_at_least_once(
            ProjectionId("counter-view", "baseline"),
            entity_type="counter",
            min_slice=0,
            max_slice=1023,
            handler=resumed_handler,
            initial_offset=1,
        )
        assert baseline.wait_started(1.0)
        assert baseline.offset == 1
        assert not resumed.wait(0.05)

        persist(store, persistence_id, 2, b"2")
        assert resumed.wait(1.0)
        wait_until(lambda: baseline.offset == 2)
        baseline.stop(1.0)
    finally:
        system.stop()


def test_projection_batches_are_bounded_by_retained_state_bytes(tmp_path) -> None:
    system = create_system(
        "projection-byte-batches",
        str(tmp_path / "byte-batches.sqlite3"),
        persistence={"max-state-bytes": 32},
        projection={"batch-byte-capacity": 40},
    )
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "byte-batches")
    for revision in range(1, 4):
        persist(store, persistence_id, revision, b"12345678")
    batches = Queue()
    completed = Event()
    processed = 0

    async def handler(changes) -> None:
        nonlocal processed
        batches.put_nowait(len(changes))
        processed += len(changes)
        if processed == 3:
            completed.set()

    projection = PROJECTIONS.get(system).run_at_least_once(
        ProjectionId("counter-view", "byte-batches"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
        batch_size=10,
    )
    try:
        assert projection.wait_started(1.0), projection.failure
        assert completed.wait(1.0), projection.failure
        assert batches.get_nowait() == 2
        assert batches.get_nowait() == 1
    finally:
        projection.stop(1.0)
        system.stop()


def test_empty_slice_source_advances_in_bounded_offset_windows(tmp_path) -> None:
    system = create_system("projection-scan-window", str(tmp_path / "scan-window.sqlite3"))
    store = DURABLE_STATE.get(system).store
    persistence_id = PersistenceId("counter", "scan-window")
    excluded_slice = (persistence_slice(persistence_id) + 1) % 1024
    for revision in range(1, 4):
        persist(store, persistence_id, revision, str(revision).encode("ascii"))

    first = store.changes(
        "counter",
        min_slice=excluded_slice,
        max_slice=excluded_slice,
        after_offset=0,
        limit=10,
        scan_limit=2,
    ).result(1.0)
    second = store.changes(
        "counter",
        min_slice=excluded_slice,
        max_slice=excluded_slice,
        after_offset=first.offset,
        limit=10,
        scan_limit=2,
    ).result(1.0)
    try:
        assert first.changes == ()
        assert first.offset == 2
        assert second.changes == ()
        assert second.offset == 3
    finally:
        system.stop()


def test_projection_operations_share_persistence_operation_capacity(tmp_path) -> None:
    path = str(tmp_path / "projection-operation-capacity.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE projection_effect (value INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()
    system = create_system(
        "projection-operation-capacity",
        path,
        persistence={"operation-capacity": 1},
    )
    store = DURABLE_STATE.get(system).store
    first_id = PersistenceId("counter", "projection-capacity")
    persist(store, first_id, 1, b"1")
    entered = Event()
    release = Event()

    async def handler(transaction, changes) -> None:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.005)

    projection = PROJECTIONS.get(system).run_exactly_once(
        ProjectionId("counter-view", "operation-capacity"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert entered.wait(1.0)
        with pytest.raises(PersistenceCapacityError):
            store.upsert(
                PersistenceId("counter", "rejected"),
                expected_revision=0,
                operation_id=OperationId.random(),
                manifest="counter/v1",
                payload=b"rejected",
            )
    finally:
        release.set()
        projection.stop(1.0)
        system.stop()


def test_stopping_exactly_once_projection_cancels_handler_and_rolls_back(
    tmp_path,
) -> None:
    path = str(tmp_path / "projection-handler-stop.sqlite3")
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE projection_effect (value INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()
    system = create_system("projection-handler-stop", path)
    store = DURABLE_STATE.get(system).store
    persist(store, PersistenceId("counter", "handler-stop"), 1, b"1")
    entered = Event()

    async def handler(transaction, changes) -> None:
        await transaction.execute("INSERT INTO projection_effect VALUES (1)")
        entered.set()
        await asyncio.Event().wait()

    projection = PROJECTIONS.get(system).run_exactly_once(
        ProjectionId("counter-view", "handler-stop"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert entered.wait(1.0)
        projection.stop(1.0)

        connection = sqlite3.connect(path)
        try:
            count = connection.execute("SELECT count(*) FROM projection_effect").fetchone()[0]
        finally:
            connection.close()
        assert count == 0
    finally:
        system.stop()


def test_projection_retries_registration_after_a_transient_store_lock(tmp_path) -> None:
    path = str(tmp_path / "projection-registration-retry.sqlite3")
    system = create_system(
        "projection-registration-retry",
        path,
        persistence={"operation-timeout": 0.05},
    )
    locker = sqlite3.connect(path, isolation_level=None)
    locker.execute("BEGIN EXCLUSIVE")

    async def handler(changes) -> None:
        pass

    projection = PROJECTIONS.get(system).run_at_least_once(
        ProjectionId("counter-view", "registration-retry"),
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        time.sleep(0.1)
        locker.rollback()
        locker.close()
        locker = None
        assert projection.wait_started(1.0)
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        projection.stop(1.0)
        system.stop()


def test_projection_identity_cannot_change_processing_mode(tmp_path) -> None:
    system = create_system("projection-mode", str(tmp_path / "projection-mode.sqlite3"))
    projections = PROJECTIONS.get(system)
    projection_id = ProjectionId("counter-view", "mode")

    async def handler(changes) -> None:
        pass

    first = projections.run_at_least_once(
        projection_id,
        entity_type="counter",
        min_slice=0,
        max_slice=1023,
        handler=handler,
    )
    try:
        assert first.wait_started(1.0)
        with pytest.raises(AttributeError):
            first.projection_id = ProjectionId("counter-view", "changed")
        first.stop(1.0)

        async def exactly_once_handler(transaction, changes) -> None:
            pass

        incompatible = projections.run_exactly_once(
            projection_id,
            entity_type="counter",
            min_slice=0,
            max_slice=1023,
            handler=exactly_once_handler,
        )
        assert incompatible.wait_stopped(1.0)
        assert isinstance(incompatible.failure, ProjectionSourceConflictError)
    finally:
        system.stop()
