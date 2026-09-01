from logging import LoggerAdapter
from typing import Any, MutableMapping

from movie.actor.context import ActorContext


class ActorLogger(LoggerAdapter):
    def set_context(self, ctx: ActorContext[Any]) -> None:
        self.ctx = ctx
        self._path = "<unnamed>"
        try:
            self._path = ctx.get_self()._path  # type: ignore
        except AttributeError:
            pass

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("actor_id", str(self.ctx.get_self().id))
        extra.setdefault("actor_path", self._path)
        return msg, kwargs
