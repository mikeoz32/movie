from threading import Lock
from movie.scheduler import Scheduler, Task
import time

m = Lock()


class MockTask(Task):
    execution_count = 0

    def __call__(self) -> None:
        super().__call__()
        with m:
            MockTask.execution_count += 1


def test_schedule_task():
    # A simple task function
    def sample_task():
        time.sleep(0.01)
        return 999 * 999

    # Create a scheduler instance
    scheduler = Scheduler()
    scheduler.start()

    # Schedule multiple tasks
    tasks = [MockTask(sample_task) for _ in range(120)]
    for task in tasks:
        scheduler.schedule(task)

    scheduler.stop()
    assert (
        sum(worker.load() for worker in scheduler._workers if worker is not None) == 0
    )
    assert scheduler._queue.empty()
    assert MockTask.execution_count == 120
