from movie.actor.context import ActorContext
from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher
from movie.mailbox.mailbox import Mailbox

default_config = Config(
    {
        "default": {"type": "movie.mailbox.default.DefaultMailbox"},
    }
)


class MailboxManager:
    def __init__(self, config: Config) -> None:
        self._config = config.get_config("movie.mailbox") or Config({}).with_fallback(
            default_config
        )

    def create_mailbox(
        self, dispatcher: Dispatcher, actor: ActorContext, *, mailbox: str = "default"
    ) -> Mailbox:
        mailbox_config = self._config.get_config(mailbox)
        if mailbox_config is None:
            raise ValueError(f"Mailbox type '{mailbox}' not found in configuration")
        mailbox_class = mailbox_config.get_instance("type", Mailbox)
        if mailbox_class is None:
            raise ValueError(
                f"Mailbox class for type '{mailbox}' could not be instantiated"
            )
        return mailbox_class(dispatcher, actor)
