#!/usr/bin/env python
"""Test script to verify supervision logic"""

import time
from movie.actor import ActorSystem, AbstractBehavior, Behaviors, ActorContext

class FailOnceChild(AbstractBehavior[str]):
    failed_count = 0
    receive_count = 0
    has_failed_globally = False  # Track if ANY instance has failed
    
    def __init__(self, context: ActorContext[str]) -> None:
        super().__init__(context)

    @staticmethod
    def create() -> AbstractBehavior[str]:
        return Behaviors.setup(FailOnceChild)

    def receive(
        self, context: ActorContext[str], message: str
    ) -> "AbstractBehavior[str] | None":
        FailOnceChild.receive_count += 1
        context.log.info(f"Child received message: {message}, count={FailOnceChild.receive_count}, has_failed_globally={FailOnceChild.has_failed_globally}")
        
        if not FailOnceChild.has_failed_globally:
            FailOnceChild.has_failed_globally = True
            FailOnceChild.failed_count += 1
            context.log.info("Child failing for the first time")
            raise Exception("Simulated failure in Child actor")
        
        context.log.info("Child processed message successfully")
        return self

class Parent(AbstractBehavior[str]):
    def __init__(self, context: ActorContext[str]) -> None:
        super().__init__(context)
        self.child = self.context.spawn(FailOnceChild.create(), "child-actor")
        context.log.info("Parent created child actor")

    @staticmethod
    def create() -> AbstractBehavior[str]:
        return Behaviors.setup(lambda ctx: Parent(ctx))

    def receive(
        self, context: ActorContext, message: str
    ) -> "AbstractBehavior | None":
        context.log.info(f"Parent forwarding message: {message}")
        self.child.tell(f"Forwarded: {message}")
        return self

    def on_signal(self, context: ActorContext, message) -> None:
        context.log.info(f"Parent received system message: {message}")

if __name__ == "__main__":
    print("Creating actor system...")
    system = ActorSystem.create(Parent.create(), "test-system")
    
    print("Sending first message (should fail)...")
    system.tell("Message 1")
    time.sleep(0.5)
    
    print("Sending second message (should succeed after restart)...")
    system.tell("Message 2")
    time.sleep(0.5)
    
    print("Sending third message (should succeed)...")
    system.tell("Message 3")
    time.sleep(0.5)
    
    print(f"Failed count: {FailOnceChild.failed_count}")
    print(f"Receive count: {FailOnceChild.receive_count}")
    
    print("Stopping system...")
    system.stop()
    print("Done!")
