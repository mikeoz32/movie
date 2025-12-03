"""Tests for OneForOne supervision strategy"""

import time
from movie.actor import ActorSystem, AbstractBehavior, Behaviors, ActorContext


def test_supervision_restarts_failed_child():
    """Test that a failing child actor is restarted by its parent"""
    
    class FailOnceChild(AbstractBehavior[str]):
        receive_count = 0
        failed_count = 0
        success_count = 0
        has_failed = False
        
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(FailOnceChild)

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior[str] | None":
            FailOnceChild.receive_count += 1
            
            # Fail only on the first message ever received
            if not FailOnceChild.has_failed:
                FailOnceChild.has_failed = True
                FailOnceChild.failed_count += 1
                raise Exception("Simulated failure")
            
            # After restart, process messages normally
            FailOnceChild.success_count += 1
            return self

    class Parent(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.child = self.context.spawn(FailOnceChild.create(), "child")

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(Parent)

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            self.child.tell(message)
            return self

    # Reset counters
    FailOnceChild.receive_count = 0
    FailOnceChild.failed_count = 0
    FailOnceChild.success_count = 0
    FailOnceChild.has_failed = False

    system = ActorSystem.create(Parent.create(), "test-system")
    
    # Send first message - child will fail and restart
    system.tell("msg1")
    time.sleep(0.1)
    
    # Send second message - child should process successfully after restart
    system.tell("msg2")
    time.sleep(0.1)
    
    # Send third message - child should process successfully
    system.tell("msg3")
    time.sleep(0.1)
    
    system.stop()
    
    # Verify behavior
    assert FailOnceChild.receive_count == 3, f"Expected 3 messages received, got {FailOnceChild.receive_count}"
    assert FailOnceChild.failed_count == 1, f"Expected 1 failure, got {FailOnceChild.failed_count}"
    assert FailOnceChild.success_count == 2, f"Expected 2 successful messages, got {FailOnceChild.success_count}"


def test_supervision_only_restarts_failed_child():
    """Test that only the failed child is restarted (OneForOne strategy)"""
    
    class WellBehavedChild(AbstractBehavior[str]):
        message_count = 0
        
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(WellBehavedChild)

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior[str] | None":
            WellBehavedChild.message_count += 1
            return self
    
    class FailOnceChild(AbstractBehavior[str]):
        receive_count = 0
        has_failed = False
        
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(FailOnceChild)

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior[str] | None":
            FailOnceChild.receive_count += 1
            
            if not FailOnceChild.has_failed:
                FailOnceChild.has_failed = True
                raise Exception("Simulated failure")
            
            return self

    class Parent(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.good_child = self.context.spawn(WellBehavedChild.create(), "good-child")
            self.bad_child = self.context.spawn(FailOnceChild.create(), "bad-child")

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(Parent)

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            # Send to both children
            self.good_child.tell(message)
            self.bad_child.tell(message)
            return self

    # Reset counters
    WellBehavedChild.message_count = 0
    FailOnceChild.receive_count = 0
    FailOnceChild.has_failed = False

    system = ActorSystem.create(Parent.create(), "test-system")
    
    # Send messages
    system.tell("msg1")
    time.sleep(0.1)
    
    system.tell("msg2")
    time.sleep(0.1)
    
    system.stop()
    
    # Good child should have received all messages
    assert WellBehavedChild.message_count == 2, f"Expected 2 messages to good child, got {WellBehavedChild.message_count}"
    # Bad child should have received both messages (1st failed, 2nd succeeded after restart)
    assert FailOnceChild.receive_count == 2, f"Expected 2 messages to bad child, got {FailOnceChild.receive_count}"
