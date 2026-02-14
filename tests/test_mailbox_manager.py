from movie.config import Config
from movie.mailbox.manager import MailboxManager


def test_mailbox_manager_reads_mailbox_config_key():
    cfg = Config.from_dict(
        {
            "movie": {
                "mailbox": {
                    "default": {"type": "movie.mailbox.mailbox.Mailbox"},
                }
            }
        }
    )

    manager = MailboxManager(cfg)

    assert manager._config.get("default.type") == "movie.mailbox.mailbox.Mailbox"


def test_mailbox_manager_uses_default_when_mailbox_config_absent():
    cfg = Config.from_dict({})

    manager = MailboxManager(cfg)

    assert manager._config.get("default.type") == "movie.mailbox.default.DefaultMailbox"


def test_mailbox_manager_ignores_legacy_malebox_key():
    cfg = Config.from_dict(
        {
            "movie": {
                "malebox": {
                    "default": {"type": "movie.mailbox.mailbox.Mailbox"},
                }
            }
        }
    )

    manager = MailboxManager(cfg)

    assert manager._config.get("default.type") == "movie.mailbox.default.DefaultMailbox"
