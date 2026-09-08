from movie.actor.context import InternalActorContext
from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher
from movie.mailbox.mailbox import Mailbox

default_config = Config(
    {
        "default": {
            "type": "movie.mailbox.default.DefaultMailbox",
            "capacity": 100_000,
            "throughput": 100,
        },
    }
)


class MailboxManager:
    def __init__(self, config: Config) -> None:
        self._config = (config.get_config("movie.mailbox") or Config({})).with_fallback(
            default_config
        )

    def create_mailbox(
        self,
        dispatcher: Dispatcher,
        actor: InternalActorContext,
        *,
        mailbox: str = "default",
    ) -> Mailbox:
        mailbox_config = self._config.get_config(mailbox)
        if mailbox_config is None:
            raise ValueError(f"Mailbox type '{mailbox}' not found in configuration")
        mailbox_class = mailbox_config.get_instance("type", Mailbox)
        if mailbox_class is None:
            raise ValueError(
                f"Mailbox class for type '{mailbox}' could not be instantiated"
            )
        instance = mailbox_class(dispatcher, actor, mailbox_config)
        if not all(
            hasattr(instance, method)
            for method in (
                "send",
                "try_send",
                "sendSystem",
                "stop_user_messages",
                "close",
            )
        ):
            raise TypeError(f"Configured mailbox '{mailbox}' does not implement Mailbox")
        if type(getattr(instance, "supports_user_suspension", None)) is not bool:
            raise TypeError(
                f"Configured mailbox '{mailbox}' must declare user-message suspension support"
            )
        return instance
