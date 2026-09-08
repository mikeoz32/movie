from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
from collections.abc import Sequence
from concurrent.futures import Future, InvalidStateError
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from movie.actor import ActorContext, ActorRef, ActorSystem, Behaviors
from movie.config import Config
from movie.mailbox.mailbox import MailboxCapacityExceeded
from movie.persistence import (
    PERSISTENCE_SLICE_COUNT,
    DurableEffect,
    DurableStateBehavior,
    DurableStateChange,
    EncodedState,
    OperationId,
    PersistenceId,
)
from movie.projection import PROJECTIONS, ProjectionId, ProjectionTransaction

ORDER_ENTITY_TYPE = "order"
ORDER_MANIFEST = "order-state/v1"


class OrderLineInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(min_length=1, max_length=128)
    quantity: int = Field(gt=0, le=1_000)
    unit_price_cents: int = Field(ge=0, le=100_000_000)


class CreateOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    lines: list[OrderLineInput] = Field(min_length=1, max_length=100)


class PaymentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: str = Field(min_length=1, max_length=128)


class ShipmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tracking_number: str = Field(min_length=1, max_length=128)


@dataclass(frozen=True, slots=True)
class OrderLine:
    sku: str
    quantity: int
    unit_price_cents: int


@dataclass(frozen=True, slots=True)
class CommandReceipt:
    operation_id: str
    fingerprint: str
    revision: int
    status: str


@dataclass(frozen=True, slots=True)
class OrderState:
    customer_id: str = ""
    lines: tuple[OrderLine, ...] = ()
    status: str = "empty"
    payment_id: str | None = None
    tracking_number: str | None = None
    receipts: tuple[CommandReceipt, ...] = ()

    @property
    def total_cents(self) -> int:
        return sum(line.quantity * line.unit_price_cents for line in self.lines)


@dataclass(frozen=True, slots=True)
class CommandResult:
    operation_id: OperationId
    accepted: bool
    revision: int
    status: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class PlaceOrder:
    customer_id: str
    lines: tuple[OrderLine, ...]
    operation_id: OperationId
    reply: Future[CommandResult]


@dataclass(frozen=True, slots=True)
class RecordPayment:
    payment_id: str
    operation_id: OperationId
    reply: Future[CommandResult]


@dataclass(frozen=True, slots=True)
class ShipOrder:
    tracking_number: str
    operation_id: OperationId
    reply: Future[CommandResult]


@dataclass(frozen=True, slots=True)
class ArchiveOrder:
    operation_id: OperationId
    reply: Future[CommandResult]


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
            "receipts": [
                {
                    "fingerprint": receipt.fingerprint,
                    "operation_id": receipt.operation_id,
                    "revision": receipt.revision,
                    "status": receipt.status,
                }
                for receipt in state.receipts
            ],
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
            receipts=tuple(
                CommandReceipt(**receipt) for receipt in document["receipts"]
            ),
        )


class OrderBehavior(DurableStateBehavior[OrderCommand, OrderState]):
    def __init__(
        self,
        context: ActorContext[OrderCommand],
        order_id: str,
    ) -> None:
        super().__init__(
            context,
            PersistenceId(ORDER_ENTITY_TYPE, order_id),
            OrderCodec(),
        )

    def empty_state(self) -> OrderState:
        return OrderState()

    def handle_command(
        self,
        state: OrderState,
        command: OrderCommand,
        context: ActorContext[OrderCommand],
    ) -> DurableEffect[OrderState]:
        fingerprint = _command_fingerprint(command)
        previous = next(
            (
                receipt
                for receipt in state.receipts
                if receipt.operation_id == str(command.operation_id)
            ),
            None,
        )
        if previous is not None:
            if previous.fingerprint != fingerprint:
                return self._reject(command, "Idempotency-Key was used for another command")
            return self.none().then_run(
                lambda current: _set_reply(
                    command.reply,
                    CommandResult(
                        command.operation_id,
                        True,
                        previous.revision,
                        previous.status,
                    ),
                )
            )

        if isinstance(command, PlaceOrder):
            if self.revision > 0:
                return self._reject(command, "order ID already exists")
            next_state = OrderState(
                customer_id=command.customer_id,
                lines=command.lines,
                status="awaiting-payment",
            )
            return self._persist(next_state, command, fingerprint)

        if isinstance(command, RecordPayment):
            if state.status != "awaiting-payment":
                return self._reject(command, "order is not awaiting payment")
            return self._persist(
                replace(state, status="paid", payment_id=command.payment_id),
                command,
                fingerprint,
            )

        if isinstance(command, ShipOrder):
            if state.status != "paid":
                return self._reject(command, "order is not paid")
            return self._persist(
                replace(
                    state,
                    status="shipped",
                    tracking_number=command.tracking_number,
                ),
                command,
                fingerprint,
            )

        if state.status == "empty":
            if self.revision == 0:
                return self._reject(command, "order does not exist")
            return self.none().then_run(
                lambda current: _set_reply(
                    command.reply,
                    CommandResult(
                        command.operation_id,
                        True,
                        self.revision,
                        "archived",
                    ),
                )
            )
        if state.status != "shipped":
            return self._reject(command, "only a shipped order can be archived")
        return self.delete(command.operation_id).then_run(
            lambda current: _set_reply(
                command.reply,
                CommandResult(command.operation_id, True, self.revision, "archived"),
            )
        )

    def _persist(
        self,
        state: OrderState,
        command: PlaceOrder | RecordPayment | ShipOrder,
        fingerprint: str,
    ) -> DurableEffect[OrderState]:
        receipt = CommandReceipt(
            str(command.operation_id),
            fingerprint,
            self.revision + 1,
            state.status,
        )
        candidate = replace(state, receipts=(*state.receipts, receipt))
        return self.persist(candidate, command.operation_id).then_run(
            lambda current: _set_reply(
                command.reply,
                CommandResult(
                    command.operation_id,
                    True,
                    receipt.revision,
                    receipt.status,
                ),
            )
        )

    def _reject(
        self,
        command: OrderCommand,
        message: str,
    ) -> DurableEffect[OrderState]:
        return self.none().then_run(
            lambda current: _set_reply(
                command.reply,
                CommandResult(
                    command.operation_id,
                    False,
                    self.revision,
                    current.status,
                    message,
                ),
            )
        )


def _set_reply(reply: Future[CommandResult], result: CommandResult) -> None:
    try:
        reply.set_result(result)
    except InvalidStateError:
        pass


def _command_fingerprint(command: OrderCommand) -> str:
    if isinstance(command, PlaceOrder):
        document = {
            "command": "place",
            "customer_id": command.customer_id,
            "lines": [
                [line.sku, line.quantity, line.unit_price_cents]
                for line in command.lines
            ],
        }
    elif isinstance(command, RecordPayment):
        document = {"command": "payment", "payment_id": command.payment_id}
    elif isinstance(command, ShipOrder):
        document = {
            "command": "shipment",
            "tracking_number": command.tracking_number,
        }
    else:
        document = {"command": "archive"}
    encoded = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
    return sha256(encoded).hexdigest()


async def update_order_summary(
    transaction: ProjectionTransaction,
    changes: tuple[DurableStateChange, ...],
) -> None:
    codec = OrderCodec()
    for change in changes:
        order_id = change.persistence_id.entity_id
        if change.deleted:
            await transaction.execute(
                "DELETE FROM order_summary WHERE order_id = ?",
                (order_id,),
            )
            continue
        assert change.manifest is not None and change.payload is not None
        state = codec.decode(change.manifest, change.payload)
        document = json.loads(change.payload)
        document.pop("receipts", None)
        document["item_count"] = sum(line.quantity for line in state.lines)
        document["total_cents"] = state.total_cents
        await transaction.execute(
            """
            INSERT INTO order_summary (
                order_id, status, revision, change_offset, document
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                status = excluded.status,
                revision = excluded.revision,
                change_offset = excluded.change_offset,
                document = excluded.document
            """,
            (
                order_id,
                state.status,
                change.revision,
                change.offset,
                json.dumps(document, separators=(",", ":"), sort_keys=True),
            ),
        )


class AuditProjection:
    def __init__(self, path: Path) -> None:
        self._path = path

    async def handle(self, changes: tuple[DurableStateChange, ...]) -> None:
        await asyncio.to_thread(self._write, changes)

    def _write(self, changes: tuple[DurableStateChange, ...]) -> None:
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
                    change.persistence_id.entity_id,
                    str(change.operation_id),
                    change.offset,
                    change.revision,
                    status,
                    change.committed_at_ns,
                )
            )
        with closing(sqlite3.connect(self._path, timeout=5.0)) as connection:
            with connection:
                connection.executemany(
                    """
                    INSERT INTO order_audit (
                        order_id, operation_id, change_offset, revision, status,
                        committed_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id, operation_id) DO NOTHING
                    """,
                    rows,
                )


class OrderRuntime:
    def __init__(self, data_dir: Path, command_timeout: float = 5.0) -> None:
        self.data_dir = data_dir
        self.movie_path = data_dir / "orders.sqlite3"
        self.audit_path = data_dir / "audit.sqlite3"
        self.command_timeout = command_timeout
        self._actors: dict[str, ActorRef[OrderCommand]] = {}
        self._actors_lock = Lock()
        self._system: ActorSystem | None = None
        self._summary = None
        self._audit = None

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        _prepare_tables(self.movie_path, self.audit_path)
        self._system = ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "durable-order-service",
            config=Config(
                {
                    "movie": {
                        "persistence": {"sqlite": {"path": str(self.movie_path)}},
                        "projection": {
                            "poll-interval": 0.05,
                            "retry-min-backoff": 0.05,
                            "retry-max-backoff": 2.0,
                        },
                    }
                }
            ),
        )
        try:
            projections = PROJECTIONS.get(self._system)
            audit = AuditProjection(self.audit_path)
            self._summary = projections.run_exactly_once(
                ProjectionId("order-summary", "all"),
                entity_type=ORDER_ENTITY_TYPE,
                min_slice=0,
                max_slice=PERSISTENCE_SLICE_COUNT - 1,
                handler=update_order_summary,
            )
            self._audit = projections.run_at_least_once(
                ProjectionId("order-audit", "all"),
                entity_type=ORDER_ENTITY_TYPE,
                min_slice=0,
                max_slice=PERSISTENCE_SLICE_COUNT - 1,
                handler=audit.handle,
            )
            for handle in (self._summary, self._audit):
                if not handle.wait_started(10.0):
                    raise handle.failure or RuntimeError("Projection failed to start")
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        if self._system is None:
            return
        for handle in (self._summary, self._audit):
            if handle is not None and handle.is_running:
                handle.request_stop()
        self._system.stop(10.0)
        self._system = None

    def execute(self, order_id: str, command_factory) -> CommandResult:
        reply: Future[CommandResult] = Future()
        actor = self._actor_for(order_id)
        try:
            actor.tell(command_factory(reply))
        except MailboxCapacityExceeded as error:
            raise HTTPException(503, "order mailbox capacity is full") from error
        try:
            return reply.result(timeout=self.command_timeout)
        except TimeoutError as error:
            reply.cancel()
            raise HTTPException(
                504,
                "command outcome is unknown; retry with the same Idempotency-Key",
            ) from error

    def get_order(self, order_id: str) -> dict | None:
        return self._get_order(order_id)

    def list_orders(self) -> list[dict]:
        return self._list_orders()

    def audit(self, order_id: str) -> list[dict]:
        return self._audit_rows(order_id)

    def compact(self) -> int:
        assert self._system is not None
        return PROJECTIONS.get(self._system).compact_changes().result(
            timeout=self.command_timeout
        )

    @property
    def health(self) -> dict:
        handles = (self._summary, self._audit)
        ready = all(
            handle is not None and handle.is_running and handle.failure is None
            for handle in handles
        )
        return {
            "status": "ready" if ready else "degraded",
            "projection_offsets": {
                str(handle.projection_id): handle.offset
                for handle in handles
                if handle is not None
            },
        }

    def _actor_for(self, order_id: str) -> ActorRef[OrderCommand]:
        with self._actors_lock:
            actor = self._actors.get(order_id)
            if actor is not None:
                return actor
            assert self._system is not None
            actor_name = f"order-{sha256(order_id.encode()).hexdigest()[:20]}"
            actor = self._system.spawn(
                Behaviors.setup(
                    lambda context: OrderBehavior(context, order_id)
                ),
                actor_name,
            )
            self._actors[order_id] = actor
            return actor

    def _get_order(self, order_id: str) -> dict | None:
        with closing(sqlite3.connect(self.movie_path, timeout=5.0)) as connection:
            row = connection.execute(
                """
                SELECT document, revision, change_offset
                FROM order_summary
                WHERE order_id = ?
                """,
                (order_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "order_id": order_id,
            **json.loads(row[0]),
            "revision": row[1],
            "change_offset": row[2],
        }

    def _list_orders(self) -> list[dict]:
        with closing(sqlite3.connect(self.movie_path, timeout=5.0)) as connection:
            rows = connection.execute(
                """
                SELECT order_id, status, revision, document
                FROM order_summary
                ORDER BY order_id
                LIMIT 100
                """
            ).fetchall()
        return [
            {
                "order_id": row[0],
                "status": row[1],
                "revision": row[2],
                "total_cents": json.loads(row[3])["total_cents"],
            }
            for row in rows
        ]

    def _audit_rows(self, order_id: str) -> list[dict]:
        with closing(sqlite3.connect(self.audit_path, timeout=5.0)) as connection:
            rows = connection.execute(
                """
                SELECT operation_id, revision, status, committed_at_ns
                FROM order_audit
                WHERE order_id = ?
                ORDER BY revision
                """,
                (order_id,),
            ).fetchall()
        return [
            {
                "operation_id": row[0],
                "revision": row[1],
                "status": row[2],
                "committed_at_ns": row[3],
            }
            for row in rows
        ]


def _prepare_tables(movie_path: Path, audit_path: Path) -> None:
    with closing(sqlite3.connect(movie_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_summary (
                order_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                revision INTEGER NOT NULL,
                change_offset INTEGER NOT NULL,
                document TEXT NOT NULL
            )
            """
        )
        connection.commit()
    with closing(sqlite3.connect(audit_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_audit (
                order_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                change_offset INTEGER NOT NULL UNIQUE,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                committed_at_ns INTEGER NOT NULL,
                PRIMARY KEY (order_id, operation_id)
            )
            """
        )
        connection.commit()


def _runtime(request: Request) -> OrderRuntime:
    return request.app.state.order_runtime


def _operation_id(value: UUID) -> OperationId:
    return OperationId(value)


def _command_response(result: CommandResult) -> dict:
    if not result.accepted:
        raise HTTPException(409, result.error)
    return {
        "operation_id": str(result.operation_id),
        "revision": result.revision,
        "status": result.status,
    }


def _settings() -> tuple[Path, float]:
    data_dir = Path(
        os.environ.get(
            "MOVIE_ORDER_DATA",
            str(Path(".movie-example") / "order-service"),
        )
    )
    command_timeout = float(os.environ.get("MOVIE_ORDER_COMMAND_TIMEOUT", "5"))
    return data_dir, command_timeout


@asynccontextmanager
async def lifespan(app: FastAPI):
    data_dir, command_timeout = _settings()
    runtime = OrderRuntime(data_dir, command_timeout)
    runtime.start()
    app.state.order_runtime = runtime
    try:
        yield
    finally:
        runtime.stop()


app = FastAPI(
    title="Movie Durable Order Service",
    version="1.0.0",
    lifespan=lifespan,
)
IdempotencyKey = Annotated[UUID, Header(alias="Idempotency-Key")]


@app.post("/orders", status_code=202)
def create_order(
    request: Request,
    body: CreateOrderInput,
    idempotency_key: IdempotencyKey,
):
    runtime = _runtime(request)
    operation_id = _operation_id(idempotency_key)
    lines = tuple(
        OrderLine(line.sku, line.quantity, line.unit_price_cents)
        for line in body.lines
    )
    result = runtime.execute(
        body.order_id,
        lambda reply: PlaceOrder(
            body.customer_id,
            lines,
            operation_id,
            reply,
        ),
    )
    return {
        "order_id": body.order_id,
        **_command_response(result),
    }


@app.post("/orders/{order_id}/payments", status_code=202)
def record_payment(
    order_id: str,
    request: Request,
    body: PaymentInput,
    idempotency_key: IdempotencyKey,
):
    operation_id = _operation_id(idempotency_key)
    result = _runtime(request).execute(
        order_id,
        lambda reply: RecordPayment(body.payment_id, operation_id, reply),
    )
    return _command_response(result)


@app.post("/orders/{order_id}/shipments", status_code=202)
def ship_order(
    order_id: str,
    request: Request,
    body: ShipmentInput,
    idempotency_key: IdempotencyKey,
):
    operation_id = _operation_id(idempotency_key)
    result = _runtime(request).execute(
        order_id,
        lambda reply: ShipOrder(body.tracking_number, operation_id, reply),
    )
    return _command_response(result)


@app.delete("/orders/{order_id}", status_code=202)
def archive_order(
    order_id: str,
    request: Request,
    idempotency_key: IdempotencyKey,
):
    operation_id = _operation_id(idempotency_key)
    result = _runtime(request).execute(
        order_id,
        lambda reply: ArchiveOrder(operation_id, reply),
    )
    return _command_response(result)


@app.get("/orders/{order_id}")
def get_order(order_id: str, request: Request):
    order = _runtime(request).get_order(order_id)
    if order is None:
        raise HTTPException(404, "order is not available in the read model")
    return order


@app.get("/orders")
def list_orders(request: Request):
    return {"orders": _runtime(request).list_orders()}


@app.get("/orders/{order_id}/audit")
def order_audit(order_id: str, request: Request):
    return {"changes": _runtime(request).audit(order_id)}


@app.get("/health")
def health(request: Request):
    return _runtime(request).health


@app.post("/admin/compact")
def compact_change_feed(request: Request):
    return {"deleted_changes": _runtime(request).compact()}


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the durable order FastAPI app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(".movie-example") / "order-service",
    )
    arguments = parser.parse_args(argv)
    os.environ["MOVIE_ORDER_DATA"] = str(arguments.data_dir)

    import uvicorn

    uvicorn.run(app, host=arguments.host, port=arguments.port)


if __name__ == "__main__":
    main()
