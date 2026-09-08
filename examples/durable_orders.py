from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from queue import Queue
from uuid import uuid4

from movie.actor import ActorContext, ActorSystem, Behaviors
from movie.config import Config
from movie.persistence import (
    DURABLE_STATE,
    PERSISTENCE_SLICE_COUNT,
    DurableEffect,
    DurableStateBehavior,
    DurableStateChange,
    EncodedState,
    OperationId,
    PersistenceId,
    persistence_slice,
)
from movie.projection import (
    PROJECTIONS,
    ProjectionBaselineError,
    ProjectionId,
    ProjectionTransaction,
)

ORDER_ENTITY_TYPE = "order"
ORDER_MANIFEST = "order-state/v1"


@dataclass(frozen=True, slots=True)
class OrderLine:
    sku: str
    quantity: int
    unit_price_cents: int


@dataclass(frozen=True, slots=True)
class OrderState:
    customer_id: str = ""
    lines: tuple[OrderLine, ...] = ()
    status: str = "empty"
    payment_id: str | None = None
    tracking_number: str | None = None

    @property
    def total_cents(self) -> int:
        return sum(line.quantity * line.unit_price_cents for line in self.lines)


@dataclass(frozen=True, slots=True)
class OrderAck:
    operation_id: OperationId
    revision: int
    status: str


@dataclass(frozen=True, slots=True)
class OrderRecovered:
    state: OrderState
    revision: int


@dataclass(frozen=True, slots=True)
class PlaceOrder:
    customer_id: str
    lines: tuple[OrderLine, ...]
    operation_id: OperationId
    reply_to: Queue[OrderAck]


@dataclass(frozen=True, slots=True)
class RecordPayment:
    payment_id: str
    operation_id: OperationId
    reply_to: Queue[OrderAck]


@dataclass(frozen=True, slots=True)
class ShipOrder:
    tracking_number: str
    operation_id: OperationId
    reply_to: Queue[OrderAck]


@dataclass(frozen=True, slots=True)
class ArchiveOrder:
    operation_id: OperationId
    reply_to: Queue[OrderAck]


OrderCommand = PlaceOrder | RecordPayment | ShipOrder | ArchiveOrder


class OrderCodec:
    def encode(self, state: OrderState) -> EncodedState:
        document = {
            "customer_id": state.customer_id,
            "lines": [
                {
                    "quantity": line.quantity,
                    "sku": line.sku,
                    "unit_price_cents": line.unit_price_cents,
                }
                for line in state.lines
            ],
            "payment_id": state.payment_id,
            "status": state.status,
            "tracking_number": state.tracking_number,
        }
        return EncodedState(
            ORDER_MANIFEST,
            json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

    def decode(self, manifest: str, payload: bytes) -> OrderState:
        if manifest != ORDER_MANIFEST:
            raise ValueError(f"unsupported order manifest: {manifest}")
        document = json.loads(payload)
        return OrderState(
            customer_id=document["customer_id"],
            lines=tuple(OrderLine(**line) for line in document["lines"]),
            status=document["status"],
            payment_id=document["payment_id"],
            tracking_number=document["tracking_number"],
        )


class OrderBehavior(DurableStateBehavior[OrderCommand, OrderState]):
    def __init__(
        self,
        context: ActorContext[OrderCommand],
        order_id: str,
        recovered: Queue[OrderRecovered],
    ) -> None:
        self._recovered = recovered
        super().__init__(context, PersistenceId(ORDER_ENTITY_TYPE, order_id), OrderCodec())

    def empty_state(self) -> OrderState:
        return OrderState()

    def on_recovery_completed(self, state: OrderState, revision: int) -> None:
        self._recovered.put_nowait(OrderRecovered(state, revision))

    def handle_command(
        self,
        state: OrderState,
        command: OrderCommand,
        context: ActorContext[OrderCommand],
    ) -> DurableEffect[OrderState]:
        if isinstance(command, PlaceOrder):
            if state.status != "empty":
                raise ValueError("an order can only be placed once")
            if not command.lines or any(line.quantity <= 0 for line in command.lines):
                raise ValueError("an order requires positive line quantities")
            next_state = OrderState(
                customer_id=command.customer_id,
                lines=command.lines,
                status="awaiting-payment",
            )
            return self._persist_with_ack(next_state, command)

        if isinstance(command, RecordPayment):
            if state.status == "awaiting-payment":
                next_state = replace(
                    state,
                    status="paid",
                    payment_id=command.payment_id,
                )
            elif state.status == "paid" and state.payment_id == command.payment_id:
                # The identical replacement lets the Store recognize an uncertain retry.
                next_state = state
            else:
                raise ValueError("only an awaiting-payment order can be paid")
            return self._persist_with_ack(next_state, command)

        if isinstance(command, ShipOrder):
            if state.status != "paid":
                raise ValueError("only a paid order can be shipped")
            return self._persist_with_ack(
                replace(
                    state,
                    status="shipped",
                    tracking_number=command.tracking_number,
                ),
                command,
            )

        if state.status != "awaiting-payment":
            raise ValueError("only an abandoned awaiting-payment order can be archived")
        return self.delete(command.operation_id).then_run(
            lambda current: command.reply_to.put_nowait(
                OrderAck(command.operation_id, self.revision, current.status)
            )
        )

    def _persist_with_ack(
        self,
        state: OrderState,
        command: PlaceOrder | RecordPayment | ShipOrder,
    ) -> DurableEffect[OrderState]:
        return self.persist(state, command.operation_id).then_run(
            lambda current: command.reply_to.put_nowait(
                OrderAck(command.operation_id, self.revision, current.status)
            )
        )


class OrderSummaryProjection:
    def __init__(self, *, fail_first_attempt: bool) -> None:
        self.attempts = 0
        self.retry_injected = False
        self._fail_first_attempt = fail_first_attempt

    async def handle(
        self,
        transaction: ProjectionTransaction,
        changes: tuple[DurableStateChange, ...],
    ) -> None:
        self.attempts += 1
        codec = OrderCodec()
        for change in changes:
            order_id = change.persistence_id.entity_id
            if change.deleted:
                await transaction.execute(
                    "DELETE FROM order_summary WHERE order_id = ?",
                    (order_id,),
                )
                notification_type = "order-archived"
                payload = json.dumps({"order_id": order_id, "revision": change.revision})
            else:
                assert change.manifest is not None and change.payload is not None
                state = codec.decode(change.manifest, change.payload)
                await transaction.execute(
                    """
                    INSERT INTO order_summary (
                        order_id, customer_id, status, item_count, total_cents,
                        revision, tracking_number
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        customer_id = excluded.customer_id,
                        status = excluded.status,
                        item_count = excluded.item_count,
                        total_cents = excluded.total_cents,
                        revision = excluded.revision,
                        tracking_number = excluded.tracking_number
                    """,
                    (
                        order_id,
                        state.customer_id,
                        state.status,
                        sum(line.quantity for line in state.lines),
                        state.total_cents,
                        change.revision,
                        state.tracking_number,
                    ),
                )
                notification_type = f"order-{state.status}"
                payload = change.payload.decode("utf-8")

            await transaction.execute(
                """
                INSERT INTO order_projection_history (order_id, revision, status)
                VALUES (?, ?, ?)
                """,
                (order_id, change.revision, notification_type),
            )
            await transaction.execute(
                """
                INSERT INTO order_outbox (
                    operation_id, order_id, notification_type, payload
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(order_id, operation_id) DO NOTHING
                """,
                (str(change.operation_id), order_id, notification_type, payload),
            )

        if self._fail_first_attempt:
            self._fail_first_attempt = False
            self.retry_injected = True
            raise RuntimeError("simulated crash before exactly-once offset commit")


class ExternalAuditProjection:
    def __init__(self, path: Path, *, fail_first_attempt: bool) -> None:
        self.attempts = 0
        self.retry_injected = False
        self._path = path
        self._fail_first_attempt = fail_first_attempt

    async def handle(self, changes: tuple[DurableStateChange, ...]) -> None:
        self.attempts += 1
        await asyncio.to_thread(self._write_idempotently, changes)
        if self._fail_first_attempt:
            self._fail_first_attempt = False
            self.retry_injected = True
            raise RuntimeError("simulated crash after at-least-once external commit")

    def _write_idempotently(self, changes: tuple[DurableStateChange, ...]) -> None:
        codec = OrderCodec()
        rows = []
        for change in changes:
            if change.deleted:
                status = "archived"
            else:
                assert change.manifest is not None and change.payload is not None
                status = codec.decode(change.manifest, change.payload).status
            rows.append(
                (
                    str(change.operation_id),
                    change.offset,
                    change.persistence_id.entity_id,
                    change.revision,
                    status,
                    change.committed_at_ns,
                )
            )
        with closing(sqlite3.connect(self._path)) as connection:
            with connection:
                connection.executemany(
                    """
                    INSERT INTO external_order_audit (
                        operation_id, change_offset, order_id, revision, status,
                        committed_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id, operation_id) DO NOTHING
                    """,
                    rows,
                )


@dataclass(frozen=True, slots=True)
class DemoResult:
    order_id: str
    order_slice: int
    duplicate_payment_revision: int
    recovered_revision: int
    final_revision: int
    final_status: str
    item_count: int
    total_cents: int
    audit_statuses: tuple[str, ...]
    audit_entry_count: int
    outbox_entry_count: int
    history_entry_count: int
    exactly_once_retry_observed: bool
    at_least_once_retry_observed: bool
    compacted_changes: int
    compaction_batches: tuple[int, ...]
    resumed_projection_offset: int
    tombstone_revision: int
    baseline_rejection_observed: bool
    baseline_offset: int


def _prepare_databases(movie_path: Path, audit_path: Path) -> None:
    movie_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(movie_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS order_summary (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                status TEXT NOT NULL,
                item_count INTEGER NOT NULL,
                total_cents INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                tracking_number TEXT
            );
            CREATE TABLE IF NOT EXISTS order_projection_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS order_outbox (
                operation_id TEXT NOT NULL,
                order_id TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (order_id, operation_id)
            );
            """
        )
    with closing(sqlite3.connect(audit_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_order_audit (
                operation_id TEXT NOT NULL,
                change_offset INTEGER NOT NULL UNIQUE,
                order_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                committed_at_ns INTEGER NOT NULL,
                PRIMARY KEY (order_id, operation_id)
            )
            """
        )
        connection.commit()


def _remove_database(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def _create_system(name: str, movie_path: Path) -> ActorSystem:
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=Config(
            {
                "movie": {
                    "persistence": {"sqlite": {"path": str(movie_path)}},
                    "projection": {
                        "batch-size": 2,
                        "compaction-batch-size": 2,
                        "poll-interval": 0.01,
                        "retry-min-backoff": 0.01,
                        "retry-max-backoff": 0.05,
                    },
                }
            }
        ),
    )


def _start_projections(
    system: ActorSystem,
    audit_path: Path,
    *,
    fail_first_attempt: bool,
) -> tuple:
    projections = PROJECTIONS.get(system)
    summary_handler = OrderSummaryProjection(fail_first_attempt=fail_first_attempt)
    audit_handler = ExternalAuditProjection(
        audit_path,
        fail_first_attempt=fail_first_attempt,
    )
    summary = projections.run_exactly_once(
        ProjectionId("order-summary", "all"),
        entity_type=ORDER_ENTITY_TYPE,
        min_slice=0,
        max_slice=PERSISTENCE_SLICE_COUNT - 1,
        handler=summary_handler.handle,
    )
    audit = projections.run_at_least_once(
        ProjectionId("order-audit", "all"),
        entity_type=ORDER_ENTITY_TYPE,
        min_slice=0,
        max_slice=PERSISTENCE_SLICE_COUNT - 1,
        handler=audit_handler.handle,
    )
    for handle in (summary, audit):
        if not handle.wait_started(5.0):
            raise handle.failure or TimeoutError(f"Projection {handle.projection_id} did not start")
    return summary, audit, summary_handler, audit_handler


def _wait_for_offset(handle, target: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while handle.offset < target:
        if handle.failure is not None:
            raise handle.failure
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Projection {handle.projection_id} did not reach {target}")
        time.sleep(0.01)


def _send_and_wait(actor, command: OrderCommand) -> OrderAck:
    actor.tell(command)
    return command.reply_to.get(timeout=5.0)


async def _ignore_changes(changes: tuple[DurableStateChange, ...]) -> None:
    pass


def run_demo(
    data_dir: Path,
    *,
    order_id: str | None = None,
    reset: bool = False,
    emit: Callable[[str], None] = print,
) -> DemoResult:
    data_dir = Path(data_dir)
    movie_path = data_dir / "orders.sqlite3"
    audit_path = data_dir / "external-audit.sqlite3"
    if reset:
        _remove_database(movie_path)
        _remove_database(audit_path)
    _prepare_databases(movie_path, audit_path)

    order_id = order_id or f"order-{uuid4().hex[:8]}"
    abandoned_order_id = f"{order_id}-abandoned"
    order_replies: Queue[OrderAck] = Queue()
    abandoned_replies: Queue[OrderAck] = Queue()
    phase_one_summary_handler = None
    phase_one_audit_handler = None
    phase_one_summary_offset = -1
    phase_one_audit_offset = -1

    first_system = _create_system("durable-orders-phase-one", movie_path)
    first_handles = []
    try:
        summary, audit, phase_one_summary_handler, phase_one_audit_handler = (
            _start_projections(first_system, audit_path, fail_first_attempt=True)
        )
        first_handles.extend((summary, audit))
        summary_start = summary.offset
        audit_start = audit.offset

        order_recovered: Queue[OrderRecovered] = Queue()
        order = first_system.spawn(
            Behaviors.setup(
                lambda context: OrderBehavior(context, order_id, order_recovered)
            ),
            "active-order",
        )
        assert order_recovered.get(timeout=5.0).revision == 0

        lines = (
            OrderLine("mechanical-keyboard", 1, 12_500),
            OrderLine("usb-c-cable", 2, 1_800),
        )
        # Operation Identity is scoped by Persistence Identity, not globally.
        shared_place_operation_id = OperationId.random()
        place = PlaceOrder(
            "customer-42",
            lines,
            shared_place_operation_id,
            order_replies,
        )
        assert _send_and_wait(order, place).revision == 1

        payment_operation_id = OperationId.random()
        lost_payment_replies: Queue[OrderAck] = Queue()
        order.tell(
            RecordPayment(
                "payment-9001",
                payment_operation_id,
                lost_payment_replies,
            )
        )
        duplicate_payment = _send_and_wait(
            order,
            RecordPayment(
                "payment-9001",
                payment_operation_id,
                order_replies,
            ),
        )

        abandoned_recovered: Queue[OrderRecovered] = Queue()
        abandoned = first_system.spawn(
            Behaviors.setup(
                lambda context: OrderBehavior(
                    context,
                    abandoned_order_id,
                    abandoned_recovered,
                )
            ),
            "abandoned-order",
        )
        assert abandoned_recovered.get(timeout=5.0).revision == 0
        abandoned_place = PlaceOrder(
            "customer-99",
            (OrderLine("reserved-item", 1, 2_000),),
            shared_place_operation_id,
            abandoned_replies,
        )
        assert _send_and_wait(abandoned, abandoned_place).revision == 1
        archive = ArchiveOrder(OperationId.random(), abandoned_replies)
        assert _send_and_wait(abandoned, archive).revision == 2

        _wait_for_offset(summary, summary_start + 4)
        _wait_for_offset(audit, audit_start + 4)
        phase_one_summary_offset = summary.offset
        phase_one_audit_offset = audit.offset
    finally:
        for handle in first_handles:
            if handle.is_running:
                handle.request_stop()
        first_system.stop(10.0)

    second_system = _create_system("durable-orders-phase-two", movie_path)
    second_handles = []
    compacted_changes = 0
    compaction_batches = []
    baseline_rejection_observed = False
    try:
        summary, audit, _, _ = _start_projections(
            second_system,
            audit_path,
            fail_first_attempt=False,
        )
        second_handles.extend((summary, audit))
        summary_start = summary.offset
        audit_start = audit.offset
        if (
            summary_start != phase_one_summary_offset
            or audit_start != phase_one_audit_offset
        ):
            raise RuntimeError("the Projections did not resume from their stored offsets")

        recovered_states: Queue[OrderRecovered] = Queue()
        recovered_order = second_system.spawn(
            Behaviors.setup(
                lambda context: OrderBehavior(context, order_id, recovered_states)
            ),
            "recovered-order",
        )
        recovered = recovered_states.get(timeout=5.0)
        if recovered.state.status != "paid" or recovered.revision != 2:
            raise RuntimeError("the order did not recover its paid state")
        tombstone = DURABLE_STATE.get(second_system).store.load(
            PersistenceId(ORDER_ENTITY_TYPE, abandoned_order_id)
        ).result(5.0)
        if tombstone is None or not tombstone.deleted or tombstone.revision != 2:
            raise RuntimeError("the abandoned order did not retain its tombstone")

        shipped = _send_and_wait(
            recovered_order,
            ShipOrder("TRACK-123", OperationId.random(), order_replies),
        )
        if shipped.revision != 3:
            raise RuntimeError("the shipped order has an unexpected Revision")
        _wait_for_offset(summary, summary_start + 1)
        _wait_for_offset(audit, audit_start + 1)
        baseline_offset = min(summary.offset, audit.offset)

        summary.stop(5.0)
        audit.stop(5.0)
        while True:
            compacted = PROJECTIONS.get(second_system).compact_changes().result(5.0)
            if compacted == 0:
                break
            compacted_changes += compacted
            compaction_batches.append(compacted)

        missing_baseline = PROJECTIONS.get(second_system).run_at_least_once(
            ProjectionId("order-baseline-missing", order_id),
            entity_type=ORDER_ENTITY_TYPE,
            min_slice=0,
            max_slice=PERSISTENCE_SLICE_COUNT - 1,
            handler=_ignore_changes,
        )
        second_handles.append(missing_baseline)
        if not missing_baseline.wait_stopped(5.0):
            raise TimeoutError("missing-baseline Projection did not stop")
        if not isinstance(missing_baseline.failure, ProjectionBaselineError):
            raise RuntimeError("compacted history did not require an explicit baseline")
        baseline_rejection_observed = True

        baseline_id = ProjectionId("order-baseline-check", order_id)
        baseline = PROJECTIONS.get(second_system).run_at_least_once(
            baseline_id,
            entity_type=ORDER_ENTITY_TYPE,
            min_slice=0,
            max_slice=PERSISTENCE_SLICE_COUNT - 1,
            handler=_ignore_changes,
            initial_offset=baseline_offset,
        )
        second_handles.append(baseline)
        if not baseline.wait_started(5.0):
            raise baseline.failure or TimeoutError("baseline Projection did not start")
        baseline.stop(5.0)
        if not PROJECTIONS.get(second_system).retire(baseline_id).result(5.0):
            raise RuntimeError("baseline Projection was not retired")
    finally:
        for handle in second_handles:
            if handle.is_running:
                handle.request_stop()
        second_system.stop(10.0)

    with closing(sqlite3.connect(movie_path)) as connection:
        summary_row = connection.execute(
            """
            SELECT status, item_count, total_cents, revision
            FROM order_summary
            WHERE order_id = ?
            """,
            (order_id,),
        ).fetchone()
        abandoned_summary = connection.execute(
            "SELECT 1 FROM order_summary WHERE order_id = ?",
            (abandoned_order_id,),
        ).fetchone()
        outbox_entry_count = connection.execute(
            "SELECT count(*) FROM order_outbox WHERE order_id IN (?, ?)",
            (order_id, abandoned_order_id),
        ).fetchone()[0]
        history_entry_count = connection.execute(
            "SELECT count(*) FROM order_projection_history WHERE order_id IN (?, ?)",
            (order_id, abandoned_order_id),
        ).fetchone()[0]
    if summary_row is None or abandoned_summary is not None:
        raise RuntimeError("the exactly-once order summary is inconsistent")

    with closing(sqlite3.connect(audit_path)) as connection:
        audit_statuses = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT status FROM external_order_audit
                WHERE order_id = ?
                ORDER BY revision
                """,
                (order_id,),
            )
        )
        audit_entry_count = connection.execute(
            "SELECT count(*) FROM external_order_audit WHERE order_id IN (?, ?)",
            (order_id, abandoned_order_id),
        ).fetchone()[0]

    status, item_count, total_cents, final_revision = summary_row
    assert phase_one_summary_handler is not None
    assert phase_one_audit_handler is not None
    result = DemoResult(
        order_id=order_id,
        order_slice=persistence_slice(PersistenceId(ORDER_ENTITY_TYPE, order_id)),
        duplicate_payment_revision=duplicate_payment.revision,
        recovered_revision=recovered.revision,
        final_revision=final_revision,
        final_status=status,
        item_count=item_count,
        total_cents=total_cents,
        audit_statuses=audit_statuses,
        audit_entry_count=audit_entry_count,
        outbox_entry_count=outbox_entry_count,
        history_entry_count=history_entry_count,
        exactly_once_retry_observed=phase_one_summary_handler.retry_injected,
        at_least_once_retry_observed=phase_one_audit_handler.retry_injected,
        compacted_changes=compacted_changes,
        compaction_batches=tuple(compaction_batches),
        resumed_projection_offset=phase_one_summary_offset,
        tombstone_revision=tombstone.revision,
        baseline_rejection_observed=baseline_rejection_observed,
        baseline_offset=baseline_offset,
    )
    emit(
        f"{result.order_id} recovered at revision {result.recovered_revision}, "
        f"then reached {result.final_status} at revision {result.final_revision}."
    )
    emit(
        f"Summary: {result.item_count} items, ${result.total_cents / 100:.2f}; "
        f"audit={result.audit_entry_count}, outbox={result.outbox_entry_count}, "
        f"history={result.history_entry_count}."
    )
    emit(
        "Both retry paths were observed without duplicate effects; "
        f"compacted {result.compacted_changes} changes through offset "
        f"{result.baseline_offset}."
    )
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the Movie durable order example")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(".movie-example") / "durable-orders",
        help="directory for the Movie and external audit SQLite databases",
    )
    parser.add_argument(
        "--order-id",
        help="fixed order ID; use --reset when repeating the same ID",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="delete the example databases before running",
    )
    arguments = parser.parse_args(argv)
    run_demo(
        arguments.data_dir,
        order_id=arguments.order_id,
        reset=arguments.reset,
    )


if __name__ == "__main__":
    main()
