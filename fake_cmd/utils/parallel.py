"""
Parallel utils.
"""
from types import TracebackType
from threading import RLock, Thread, Event
from abc import ABC, abstractmethod
from slime_core.utils.base import BaseList
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.typing import (
    List,
    Union,
    TYPE_CHECKING,
    Callable,
    Iterable,
    Type
)
from . import config
from .common import GreaterThanAnything, polling
if TYPE_CHECKING:
    from fake_cmd.core.server import SessionCommand


ExitCallbackFunc = Callable[
    [
        Union[Type[BaseException], None],
        Union[BaseException, None],
        Union[TracebackType, None]
    ],
    None
]


class ExitCallbacks:
    
    def __init__(
        self,
        callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        self.exit_callback_lock__ = RLock()
        self.exit_callbacks__ = BaseList(callbacks)
    
    def run_exit_callbacks__(
        self,
        __exc_type: Union[Type[BaseException], None] = None,
        __exc_value: Union[BaseException, None] = None,
        __traceback: Union[TracebackType, None] = None
    ):
        with self.exit_callback_lock__:
            for callback in self.exit_callbacks__:
                callback(__exc_type, __exc_value, __traceback)
    
    def add_exit_callback__(self, callback: ExitCallbackFunc) -> None:
        with self.exit_callback_lock__:
            self.exit_callbacks__.append(callback)


class CommandPool(ReadonlyAttr):
    
    readonly_attr__ = (
        'queue_lock',
        'execute_lock',
        'pool_close',
        'polling_thread'
    )
    
    def __init__(
        self,
        max_threads: Union[int, None] = None
    ) -> None:
        self.queue: List["SessionCommand"] = []
        self.queue_lock = RLock()
        self.execute: List["SessionCommand"] = []
        self.execute_lock = RLock()
        self.max_threads = max_threads or GreaterThanAnything()
        self.pool_close = Event()
        self.polling_thread = Thread(target=self.run)
    
    def start(self):
        return self.polling_thread.start()
    
    def run(self):
        for _ in polling(config.cmd_pool_schedule_interval):
            if self.pool_close.is_set():
                break
            
            self.schedule()
        
        for t in self.execute:
            t.join()
    
    def schedule(self):
        """
        Schedule new jobs (in batch).
        """
        # NOTE: ``with (A, B): pass`` will raise exception in Python 3.7, 
        # while ``with A, B: pass`` is ok.
        with self.queue_lock, self.execute_lock:
            # Reap finished commands (Thread or Process) so that
            # ``len(self.execute)`` reflects the true concurrency.
            self.execute = [c for c in self.execute if c.is_alive()]
            
            while (
                self.queue and 
                self.max_threads > len(self.execute)
            ):
                cmd = self.queue.pop(0)
                cmd_state = cmd.cmd_state
                if cmd_state.pending_terminate:
                    continue
                
                with cmd_state.scheduled_lock:
                    if cmd_state.pending_terminate:
                        # Double-check to make it safe.
                        continue
                
                    self.execute.append(cmd)
                    cmd.start()
                    # Using a scheduled lock, it can ensure that when 
                    # other threads check the ``scheduled`` Event, it 
                    # is consistent with the real command running state.
                    cmd_state.scheduled.set()
    
    def close(self):
        self.pool_close.set()
    
    def submit(self, cmd: "SessionCommand") -> bool:
        """
        Submit a command and return whether the command will be executed 
        immediately.
        """
        with self.queue_lock, self.execute_lock:
            self.queue.append(cmd)
            queued = self.max_threads < (len(self.queue) + len(self.execute))
            if queued:
                cmd.cmd_state.queued.set()
                cmd.info_queued()
            return (not queued)
    
    def cancel(self, command: "SessionCommand") -> bool:
        with self.queue_lock:
            if command not in self.queue:
                return False
            try:
                self.queue.remove(command)
            except ValueError:
                pass
            return True
    
    def update_and_get_execute(self) -> List["SessionCommand"]:
        with self.execute_lock:
            self.execute = list(filter(
                lambda exec: exec.is_alive(),
                self.execute
            ))
            return self.execute


class LifecycleRun(ExitCallbacks, ABC):
    """
    Separate the running process into ``before_running``, 
    ``running`` and ``after_running``.
    """
    def run(self):
        """
        This method may not be overridden.
        """
        if self.before_running():
            with self:
                self.running()
    
    def __enter__(self): return self
    
    def __exit__(
        self,
        __exc_type: Union[Type[BaseException], None],
        __exc_value: Union[BaseException, None],
        __traceback: Union[TracebackType, None]
    ):
        """
        Use exit to catch up exceptions and pass them to the functions.
        """
        self.run_exit_callbacks__(__exc_type, __exc_value, __traceback)
        self.after_running(__exc_type, __exc_value, __traceback)
    
    @abstractmethod
    def running(self): pass

    @abstractmethod
    def before_running(self) -> bool:
        """
        Execute before running. Return ``False`` to stop the 
        following ``running`` operation.
        """
        return True
    
    @abstractmethod
    def after_running(
        self,
        __exc_type: Union[Type[BaseException], None] = None,
        __exc_value: Union[BaseException, None] = None,
        __traceback: Union[TracebackType, None] = None
    ): pass
