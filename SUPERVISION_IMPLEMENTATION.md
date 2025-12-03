# OneForOne Supervision Strategy Implementation

## Summary

Successfully implemented the OneForOne supervision strategy for the `movie` actor framework. When a child actor fails, the parent actor automatically restarts it, allowing the child to continue processing messages.

## Changes Made

### 1. Added Restart System Message
- **File**: `movie/actor/system.py`
- Added `ActorSystem.Restart` dataclass to represent restart command
- Updated `SystemMessage` union type to include Restart

### 2. Implemented RestartingState
- **File**: `movie/actor/impl/context.py`
- Created `RestartingState` class to handle actor restart lifecycle
- On restart:
  - Clears message stash
  - Resets behavior to original (deferred) behavior
  - Transitions to STARTING state for reinitiali zation
  
### 3. Enhanced LocalActorContext
- **File**: `movie/actor/impl/context.py`
- Stores original behavior for restart: `self._original_behavior = behavior`
- Modified `RunningState.invoke()` to transition to FAILED state on exception
- Added Restart message handling in `RunningState.invoke_system()`
- Added Restart message handling in `FailedState.invoke_system()`

### 4. Implemented OneForOne Strategy in Parent
- **File**: `movie/actor/impl/context.py`
- Modified `on_signal()` to handle `Failed` messages
- When child fails, parent sends `Restart` message to that specific child only
- No other children are affected (OneForOne strategy)

## How It Works

1. Child actor encounters exception during message processing
2. Child transitions to FAILED state and sends `Failed` message to parent
3. Parent receives `Failed` message in `on_signal()`
4. Parent sends `Restart` message back to the failed child
5. Child receives `Restart` and transitions to RESTARTING state
6. In RESTARTING state:
   - Behavior is reset to original factory
   - Stash is cleared
   - Actor transitions to STARTING state
7. STARTING state reinitializes the actor (sends PreStart, etc.)
8. Actor returns to RUNNING state and can process messages normally

## Testing

Created verification script (`verify_supervision.py`) that demonstrates:
- Child fails on first message
- Parent restarts child
- Child successfully processes subsequent messages after restart

**Test Results**:
```
Messages received by child: 3
Times child failed: 1
Messages successfully processed: 2
✓ SUCCESS: Supervision working correctly!
```

## Known Issues

- Pre-existing issue: `ActorSystem.stop()` hangs due to dispatcher cleanup
- This issue exists in the codebase before supervision changes
- Does not affect supervision functionality itself
- Workaround: Don't call `stop()` in current tests

## Future Enhancements

Potential improvements for production use:
1. Add restart limits (e.g., max 3 restarts per minute)
2. Add different supervision strategies (AllForOne, RestForOne)
3. Add configurable restart delays (backoff)
4. Add restart statistics/monitoring
