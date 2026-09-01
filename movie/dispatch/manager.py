from threading import Condition, Event, Lock, RLock, Thread
from time import monotonic
from typing import MutableMapping

from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher, InternalDispatcher

DEFAULT_DISPATCHER_ID = "default-dispatcher"
INTERNAL_DISPATCHER_ID = "internal-dispatcher"

default_config = Config(
    {
        DEFAULT_DISPATCHER_ID: {
            "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
        },
        INTERNAL_DISPATCHER_ID: {
            "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
        },
    }
)


class DispatcherConfigurator:
    def __init__(self, config: Config) -> None:
        self._config = config

    def create_dispatcher(self) -> InternalDispatcher:
        dispatcher = self._config.get_instance("type", Dispatcher)
        if not dispatcher:
            raise ValueError("Failed to create dispatcher from config")
        instance = dispatcher(self._config)
        if not all(
            hasattr(instance, method)
            for method in ("dispatch", "dispatch_system", "start", "stop")
        ):
            raise TypeError("Configured dispatcher does not implement InternalDispatcher")
        return instance


class DispatcherManager:
    def __init__(self, config: Config):
        self._config = (
            config.get_config("movie.dispatcher") or default_config
        ).with_fallback(default_config)

        self._dispatchers: MutableMapping[str, InternalDispatcher] = {}
        self._state_changed = Condition(RLock())
        self._lock = self._state_changed
        self._stopping = False
        self._stopped = False
        self._initializing: set[str] = set()
        self._unregistering: set[str] = set()
        self._stop_jobs: dict[int, tuple[Event, list[BaseException]]] = {}
        self._stop_jobs_lock = Lock()

    def register_dispatcher(self, name: str, dispatcher: InternalDispatcher) -> None:
        with self._lock:
            if self._stopping or self._stopped:
                raise RuntimeError("Dispatcher manager is stopping or stopped")
            if name in self._dispatchers or name in self._initializing:
                raise ValueError(f"Dispatcher '{name}' is already registered")
            self._dispatchers[name] = dispatcher

    def unregister_dispatcher(
        self, name: str, timeout: float | None = None
    ) -> None:
        with self._lock:
            if self._stopping or self._stopped:
                raise RuntimeError("Dispatcher manager is stopping or stopped")
            if name in self._unregistering:
                raise RuntimeError(f"Dispatcher '{name}' is already unregistering")
            dispatcher = self._dispatchers.get(name)
            if dispatcher is not None:
                self._unregistering.add(name)
        if dispatcher is None:
            return
        stopped = False
        finalizing = False
        try:
            self._stop_dispatcher(dispatcher, timeout)
            stopped = True
        except TimeoutError:
            finalizing = True
            Thread(
                target=self._finish_unregister,
                args=(name, dispatcher),
                name="movie-dispatcher-unregister",
                daemon=True,
            ).start()
            raise
        finally:
            if not finalizing:
                with self._state_changed:
                    if stopped and self._dispatchers.get(name) is dispatcher:
                        self._dispatchers.pop(name)
                    self._unregistering.discard(name)
                    self._state_changed.notify_all()

    def lookup(self, name: str) -> Dispatcher:
        with self._state_changed:
            while name in self._initializing:
                self._state_changed.wait()
            if self._stopping or self._stopped:
                raise RuntimeError("Dispatcher manager is stopping or stopped")
            if name in self._unregistering:
                raise RuntimeError(f"Dispatcher '{name}' is unregistering")
            dispatcher = self._dispatchers.get(name, None)
            if dispatcher is not None:
                return dispatcher
            self._initializing.add(name)

        try:
            configurator = DispatcherConfigurator(
                self._config.get_config(name) or Config({})
            )
            dispatcher = configurator.create_dispatcher()
            dispatcher.start()
        except BaseException:
            with self._state_changed:
                self._initializing.discard(name)
                self._state_changed.notify_all()
            raise

        with self._state_changed:
            stopping = self._stopping or self._stopped
            if not stopping:
                self._dispatchers[name] = dispatcher
                self._initializing.discard(name)
                self._state_changed.notify_all()
                return dispatcher
        try:
            self._stop_dispatcher(dispatcher, None)
        finally:
            with self._state_changed:
                self._initializing.discard(name)
                self._state_changed.notify_all()
        raise RuntimeError("Dispatcher manager is stopping or stopped")

    def stop_all(self, timeout: float | None = None) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("Dispatcher shutdown timeout must be positive")
        deadline = None if timeout is None else monotonic() + timeout
        if deadline is None:
            self._state_changed.acquire()
        else:
            remaining = deadline - monotonic()
            if remaining <= 0 or not self._state_changed.acquire(timeout=remaining):
                raise TimeoutError("Dispatchers did not stop within the deadline")
        owns_stopping = False
        try:
            if self._stopped:
                return
            while self._stopping:
                if deadline is None:
                    self._state_changed.wait()
                else:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Dispatchers did not stop within the deadline")
                    self._state_changed.wait(remaining)
                if self._stopped:
                    return
            self._stopping = True
            owns_stopping = True
            while self._initializing or self._unregistering:
                if deadline is None:
                    self._state_changed.wait()
                else:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        self._stopping = False
                        self._state_changed.notify_all()
                        raise TimeoutError(
                            "Dispatchers did not stop within the deadline"
                        )
                    self._state_changed.wait(remaining)
            dispatchers = list(self._dispatchers.items())
            dispatcher_snapshot = dict(dispatchers)
            owns_stopping = False
        except BaseException:
            if owns_stopping:
                self._stopping = False
                self._state_changed.notify_all()
            raise
        finally:
            self._state_changed.release()
        errors = []
        timed_out = False
        stopped_names = []
        try:
            for name, dispatcher in dispatchers:
                try:
                    remaining = None if deadline is None else deadline - monotonic()
                    if remaining is not None and remaining <= 0:
                        timed_out = True
                        break
                    self._stop_dispatcher(dispatcher, remaining)
                    stopped_names.append(name)
                    if deadline is not None and monotonic() > deadline:
                        timed_out = True
                        break
                except TimeoutError as error:
                    errors.append(error)
                    timed_out = True
                    break
                except BaseException as error:
                    errors.append(error)
        finally:
            with self._state_changed:
                for name in stopped_names:
                    if self._dispatchers.get(name) is dispatcher_snapshot.get(name):
                        self._dispatchers.pop(name, None)
                self._stopped = not self._dispatchers
                self._stopping = False
                self._state_changed.notify_all()
        if timed_out:
            timeout_error = TimeoutError("Dispatchers did not stop within the deadline")
            for error in errors:
                timeout_error.add_note(repr(error))
            raise timeout_error
        if errors:
            if all(isinstance(error, Exception) for error in errors):
                raise ExceptionGroup("Failed to stop dispatchers", errors)
            raise BaseExceptionGroup("Failed to stop dispatchers", errors)

    def _stop_dispatcher(
        self, dispatcher: InternalDispatcher, timeout: float | None
    ) -> None:
        if timeout is None or getattr(dispatcher, "cooperative_shutdown", False):
            dispatcher.stop(timeout)
            return

        job_id = id(dispatcher)
        with self._stop_jobs_lock:
            job = self._stop_jobs.get(job_id)
            if job is None:
                completed = Event()
                errors: list[BaseException] = []

                def stop() -> None:
                    try:
                        dispatcher.stop(timeout)
                    except BaseException as error:
                        errors.append(error)
                    finally:
                        completed.set()

                job = (completed, errors)
                self._stop_jobs[job_id] = job
                Thread(
                    target=stop,
                    name="movie-dispatcher-stop",
                    daemon=True,
                ).start()
        completed, errors = job
        if not completed.wait(timeout):
            raise TimeoutError("Dispatcher did not stop within the deadline")
        with self._stop_jobs_lock:
            self._stop_jobs.pop(job_id, None)
        if errors:
            raise errors[0]

    def _finish_unregister(
        self, name: str, dispatcher: InternalDispatcher
    ) -> None:
        stopped = False
        try:
            if getattr(dispatcher, "cooperative_shutdown", False):
                dispatcher.stop(None)
            else:
                with self._stop_jobs_lock:
                    job = self._stop_jobs.get(id(dispatcher))
                if job is not None:
                    completed, errors = job
                    completed.wait()
                    if errors:
                        raise errors[0]
                    with self._stop_jobs_lock:
                        self._stop_jobs.pop(id(dispatcher), None)
                else:
                    dispatcher.stop(None)
            stopped = True
        except BaseException:
            pass
        finally:
            with self._state_changed:
                if stopped and self._dispatchers.get(name) is dispatcher:
                    self._dispatchers.pop(name)
                self._unregistering.discard(name)
                self._state_changed.notify_all()

    @property
    def default_dispatcher(self) -> Dispatcher:
        return self.lookup(DEFAULT_DISPATCHER_ID)

    @property
    def internal_dispatcher(self) -> Dispatcher:
        return self.lookup(INTERNAL_DISPATCHER_ID)
