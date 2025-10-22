from movie.actor import ActorContext
from movie.scheduler import Scheduler, DefaultMailbox, SingleMessageDispatchMailbox


def test_mailbox_send_and_process():
    class MockActor(ActorContext):
        def __init__(self):
            self.processed_messages = []

        def invoke(self, message):
            self.processed_messages.append(message)

    scheduler = Scheduler()
    scheduler.start()

    mock_actor = MockActor()
    mailbox = DefaultMailbox(scheduler, mock_actor)

    # Send messages to the mailbox
    messages = ["msg1", "msg2", "msg3", "msg4", "msg5"]
    for msg in messages:
        mailbox.send(msg)

    # Allow some time for processing
    scheduler.stop()

    # Verify that all messages were processed
    assert mock_actor.processed_messages == messages


def test_single_dispatch_mailbox_send_and_process():
    class MockActor(ActorContext):
        def __init__(self):
            self.processed_messages = []

        def invoke(self, message):
            self.processed_messages.append(message)

    scheduler = Scheduler()
    scheduler.start()

    mock_actor = MockActor()
    mailbox = SingleMessageDispatchMailbox(scheduler, mock_actor)

    # Send messages to the mailbox
    messages = ["msg1", "msg2", "msg3", "msg4", "msg5"]
    for msg in messages:
        mailbox.send(msg)

    # Allow some time for processing
    scheduler.stop()

    # Verify that all messages were processed
    assert mock_actor.processed_messages == messages
