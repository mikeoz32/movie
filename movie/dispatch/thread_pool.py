

from concurrent.futures import ThreadPoolExecutor
import os
from movie.dispatch.dispatcher import InternalDispatcher, Task


class ThreadPoolScheduler(InternalDispatcher):
    def __init__(self) -> None:
        self._cpu_count = os.cpu_count() or 1
        self._pool = ThreadPoolExecutor(max_workers=self._cpu_count)

    def dispatch(self, task: Task) -> None:
        try:
            self._pool.submit(task)
        except RuntimeError:
            pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        self._pool.shutdown(wait=True)
