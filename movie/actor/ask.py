from __future__ import annotations

from concurrent.futures import Future
from threading import Timer
from typing import Any, Callable
import uuid

from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext
from movie.actor.ref import ActorRef
from movie.actor.system import ActorSystem


class _AskReplyBehavior(AbstractBehavior[Any]):
    def __init__(self, context: ActorContext[Any], result_future: Future[Any]) -> None:
        super().__init__(context)
        self._future = result_future

    def receive(self, context: ActorContext[Any], message: Any):
        if not self._future.done():
            self._future.set_result(message)
        _stop_actor(context.get_self())
        return None


def _stop_actor(ref: ActorRef[Any]) -> None:
    tell_system = getattr(ref, "tell_system", None)
    if callable(tell_system):
        tell_system(ActorSystem.Stop())


def ask(
    system: ActorSystem,
    target: ActorRef[Any],
    message_factory: Callable[[ActorRef[Any]], Any],
    *,
    timeout: float | None = None,
    name: str | None = None,
) -> Future[Any]:
    """Send request message to actor and complete a Future from temporary reply actor.

    Args:
        system: actor system used to spawn temporary reply actor.
        target: actor that will receive request.
        message_factory: factory that receives reply-to ActorRef and returns request message.
        timeout: timeout in seconds. On timeout future completes with TimeoutError.
        name: optional temporary reply actor name.
    """

    result: Future[Any] = Future()
    reply_actor_name = name or f"$ask-{uuid.uuid4()}"
    reply_to = system.spawn(
        Behaviors.setup(lambda ctx: _AskReplyBehavior(ctx, result)),
        reply_actor_name,
    )

    timeout_timer: Timer | None = None
    if timeout is not None:
        def on_timeout() -> None:
            if not result.done():
                result.set_exception(TimeoutError(f"ask timeout after {timeout}s"))
                _stop_actor(reply_to)

        timeout_timer = Timer(timeout, on_timeout)
        timeout_timer.daemon = True
        timeout_timer.start()

    def _cleanup(_f: Future[Any]) -> None:
        if timeout_timer is not None:
            timeout_timer.cancel()

    result.add_done_callback(_cleanup)

    try:
        request = message_factory(reply_to)
        target.tell(request)
    except Exception as exc:
        if not result.done():
            result.set_exception(exc)
        _stop_actor(reply_to)

    return result
