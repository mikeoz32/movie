#!/usr/bin/env python
"""Test script to verify supervision logic - no stop()"""

import time
from movie.actor import ActorSystem, AbstractBehavior, Behaviors, ActorContext

class FailOnceChild(AbstractBehavior[str]):
    failed_count = 0
    receive_count = 0
    success_count = 0
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
        
        if not FailOnceChild.has_failed_globally:
            FailOnceChild.has_failed_globally = True
            FailOnceChild.failed_count += 1
            print(f"[CHILD] Failing on message: {message}")
            raise Exception("Simulated failure in Child actor")
        
        FailOnceChild.success_count += 1
        print(f"[CHILD] Successfully processed: {message}")
        return self

class Parent(AbstractBehavior[str]):
    def __init__(self, context: ActorContext[str]) -> None:
        super().__init__(context)
        self.child = self.context.spawn(FailOnceChild.create(), "child-actor")
        print("[PARENT] Created child actor")

    @staticmethod
    def create() -> AbstractBehavior[str]:
        return Behaviors.setup(lambda ctx: Parent(ctx))

    def receive(
        self, context: ActorContext, message: str
    ) -> "AbstractBehavior | None":
        print(f"[PARENT] Forwarding message to child: {message}")
        self.child.tell(f"Forwarded: {message}")
        return self

    def on_signal(self, context: ActorContext, message) -> None:
        pass  # Suppress system message logging

if __name__ == "__main__":
    print("=" * 60)
    print("SUPERVISION TEST - OneForOne Strategy")
    print("=" * 60)
    
    print("\n1. Creating actor system...")
    system = ActorSystem.create(Parent.create(), "test-system")
    time.sleep(0.1)
    
    print("\n2. Sending first message (should fail)...")
    system.tell("Message 1")
    time.sleep(0.3)
    
    print("\n3. Sending second message (should succeed after restart)...")
    system.tell("Message 2")
    time.sleep(0.3)
    
    print("\n4. Sending third message (should succeed)...")
    system.tell("Message 3")
    time.sleep(0.3)
    
    print("\n" + "=" * 60)
    print("RESULTS:")
    print("=" * 60)
    print(f"Messages received by child: {FailOnceChild.receive_count}")
    print(f"Times child failed: {FailOnceChild.failed_count}")
    print(f"Messages successfully processed: {FailOnceChild.success_count}")
    
    expected_received = 3
    expected_failed = 1
    expected_success = 2
    
    if (FailOnceChild.receive_count == expected_received and 
        FailOnceChild.failed_count == expected_failed and
        FailOnceChild.success_count == expected_success):
        print("\n✓ SUCCESS: Supervision working correctly!")
        print("  - Child failed once")
        print("  - Child was restarted")
        print("  - Child processed subsequent messages successfully")
    else:
        print(f"\n✗ FAILURE: Unexpected results")
        print(f"  Expected: received={expected_received}, failed={expected_failed}, success={expected_success}")
        print(f"  Got: received={FailOnceChild.receive_count}, failed={FailOnceChild.failed_count}, success={FailOnceChild.success_count}")
    
    print("=" * 60)
