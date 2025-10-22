class Envelope:
    """
    An envelope that holds a message and its metadata. And is Node in a linked list.
    """

    def __init__(self) -> None:
        self.next: "Envelope | None" = None
        self.prev: "Envelope | None" = None


class Mailbox:
    """
    A mailbox that holds envelopes. Each mailbox is assigned to a single actor.
    """

    def __init__(self) -> None:
        self.head: Envelope | None = None
        self.tail: Envelope | None = None

    def is_empty(self) -> bool:
        return self.head is None

    def push(self, envelope: Envelope) -> None:
        if self.tail is None:
            self.head = envelope
            self.tail = envelope
        else:
            self.tail.next = envelope
            envelope.prev = self.tail
            self.tail = envelope

    def pop(self) -> Envelope | None:
        if self.head is None:
            return None
        envelope = self.head
        self.head = envelope.next
        if self.head is not None:
            self.head.prev = None
        else:
            self.tail = None
        envelope.next = None
        envelope.prev = None
        return envelope
