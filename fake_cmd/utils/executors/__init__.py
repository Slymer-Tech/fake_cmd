"""
This module contains utils for running a command in the server.
"""

import os
import signal
from subprocess import Popen
from abc import ABC, abstractmethod, ABCMeta
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import Metaclasses, _ReadonlyAttrMetaclass
from slime_core.utils.registry import Registry
from slime_core.utils.common import FuncParams
from slime_core.utils.typing import (
    Type,
    Missing,
    Any,
    Union,
    MISSING,
    TypeVar,
    Generic,
    TYPE_CHECKING,
)
from fake_cmd.utils import config
from fake_cmd.utils.common import polling, get_control_char, resolve_classname
from fake_cmd.utils.logging import logger

if TYPE_CHECKING:
    from .reader import PopenReader, PexpectReader
    from .writer import PopenWriter


class Executor(
    ABC, ReadonlyAttr, metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    Abstract base class that defines how a command is started,
    how the output is read and the input is written, and how it
    normally finishes or is terminated.
    """

    readonly_attr__ = ("process",)

    def __init__(self) -> None:
        self.process: Union[Missing, Any] = MISSING

    def is_started(self) -> bool:
        """
        Determine whether the command has been started. By default,
        this method checks whether ``self.process`` is anything else
        or ``MISSING``.

        NOTE: This method does not reflect whether the process has
        been scheduled by the OS, and it only shows that whether the
        command object has been created (e.g., subprocess.Popen,
        pexpect.spawn, etc.).
        """
        return self.process is not MISSING

    @property
    @abstractmethod
    def pid(self) -> Any:
        """
        Pid of the process (we cannot ensure that this property always
        returns an integer value, and it can be ``None`` or anything else
        sometimes).
        """
        pass

    @abstractmethod
    def start(self) -> None:
        """
        Start a command exec (through subprocess.Popen, pexpect.spawn,
        etc.).
        """
        pass

    @abstractmethod
    def read(self, timeout: float) -> str:
        """
        Read the output content within the timeout.
        """
        pass

    @abstractmethod
    def read_all(self) -> str:
        """
        Read all the output content util file end.
        """
        pass

    def readable(self) -> bool:
        """
        Return whether the exec is readable. Because the command
        output should always be readable, the method returns True
        by default.
        """
        return True

    def close_read(self) -> None:
        """
        Close the corresponding readers (Optionally implemented).
        """
        pass

    @abstractmethod
    def write(self, content: str) -> bool:
        """
        Write the content to the input (if it is writable). Return
        ``True`` if the writing process succeeded.
        """
        pass

    @abstractmethod
    def write_line(self, content: str) -> bool:
        """
        Similar to ``write``, but add linesep at the end.
        """
        pass

    def writable(self) -> bool:
        """
        Return whether the exec is writable (i.e., whether the command
        opens input to it).
        """
        return False

    def close_write(self) -> None:
        """
        Close the corresponding writers (Optionally implemented).
        """
        pass

    def sighup(self) -> None:
        """
        Send sighup signal to the process. This is optionally implemented,
        and is not used by the fake_cmd in the current version.
        """
        pass

    @abstractmethod
    def keyboard_interrupt(self) -> None:
        """
        Send keyboard interrupt to the process.
        """
        pass

    @abstractmethod
    def terminate(self) -> None:
        """
        Terminate the process.
        """
        pass

    @abstractmethod
    def kill(self) -> None:
        """
        Kill the process.
        """
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """
        Return whether the process is still running (through Popen.poll,
        pexpect.spawn.isalive, etc.).
        """
        pass

    @abstractmethod
    def __str__(self) -> str:
        """
        For display.
        """
        pass

    #
    # Context manager usage.
    #

    def __enter__(self):
        # Automatically start here.
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        # Close read and write.
        try:
            self.close_read()
        except Exception:
            pass

        try:
            self.close_write()
        except Exception:
            pass
        # Wait for the process to terminate.
        for _ in polling(config.cmd_polling_interval):
            if not self.is_running():
                break
        return False


_ExecutorT = TypeVar("_ExecutorT", bound=Executor)


class ExecutorComponent(ReadonlyAttr, Generic[_ExecutorT]):
    """
    Executor components (such as Readers, Writers, etc.) are initialized as
    part of the Executor object, and they should bind the executor instance
    in the ``__init__`` method of the Executor object.
    """

    readonly_attr__ = ("executor",)

    def __init__(self) -> None:
        self.executor: Union[Missing, _ExecutorT] = MISSING

    def bind(self, executor: _ExecutorT) -> None:
        """
        Bind the executor after init.
        """
        self.executor = executor


#
# Popen Executor
#


class PlatformPopenExecutor(Executor):

    readonly_attr__ = ("reader", "writer", "encoding")

    def __init__(
        self,
        popen_params: FuncParams,
        reader: "PopenReader",
        writer: "PopenWriter",
        encoding: Union[str, None] = None,
    ) -> None:
        Executor.__init__(self)
        self.popen_params = popen_params
        self.reader = reader
        self.writer = writer
        self.encoding = encoding or config.cmd_pipe_encoding
        # Bind reader and writer.
        self.reader.bind(self)
        self.writer.bind(self)

    @property
    def stdout(self):
        return self.process.stdout

    @property
    def stdin(self):
        return self.process.stdin

    @property
    def stderr(self):
        return self.process.stderr

    @property
    def pid(self):
        return getattr(self.process, "pid", None)

    @property
    def poll(self):
        return self.process.poll

    @property
    def returncode(self):
        return self.process.returncode

    def start(self) -> None:
        popen_params = self.popen_params
        self.process: Union[Missing, Popen] = Popen(
            *popen_params.args, **popen_params.kwargs
        )
        # Call read_init here.
        self.reader.read_init()

    def read(self, timeout: float) -> str:
        return self.reader.read(timeout)

    def read_all(self) -> str:
        return self.reader.read_all()

    def readable(self) -> bool:
        return self.reader.readable()

    def write(self, content: str) -> bool:
        return self.writer.write(content)

    def write_line(self, content: str) -> bool:
        # Because we use bytes writing here, we can use ``os.linesep``
        # as the line separator.
        return self.write(f"{content}{os.linesep}")

    def writable(self) -> bool:
        return self.writer.writable()

    def close_write(self) -> None:
        self.writer.close()

    def keyboard_interrupt(self) -> None:
        self.send_signal(signal.SIGINT)

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        if hasattr(signal, "SIGKILL"):
            self.send_signal(signal.SIGKILL)
        else:
            self.terminate()

    @abstractmethod
    def send_signal(self, sig) -> None:
        """
        Send signal to the process.
        """
        pass

    def is_running(self) -> bool:
        return self.is_started() and self.poll() is None

    def __str__(self) -> str:
        return (
            f"{resolve_classname(self)}"
            f"(reader={str(self.reader)}, writer={str(self.writer)}, "
            f"pid={self.pid}, encoding={self.encoding})"
        )

    def __enter__(self):
        super().__enter__()
        self.process.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        res = self.process.__exit__(*args, **kwargs)
        super().__exit__(*args, **kwargs)
        return res


platform_open_executor_registry = Registry[Type[PlatformPopenExecutor]](
    "platform_open_executor_registry"
)


@platform_open_executor_registry(key="default")
class DefaultPopenExecutor(PlatformPopenExecutor):

    def send_signal(self, sig) -> None:
        """
        Safely send signal according to subprocess.Popen.send_signal.
        """
        self.poll()
        if self.returncode is not None:
            return

        # Try to kill the progress.
        try:
            os.kill(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


@platform_open_executor_registry(key="unix")
class UnixPopenExecutor(PlatformPopenExecutor):

    def __init__(
        self,
        popen_params: FuncParams,
        reader: "PopenReader",
        writer: "PopenWriter",
        encoding: Union[str, None] = None,
    ) -> None:
        # Use ``start_new_session`` to create a new process group.
        platform_kwargs = {"start_new_session": True}
        platform_kwargs.update(popen_params.kwargs)
        popen_params.kwargs = platform_kwargs
        super().__init__(
            popen_params=popen_params, reader=reader, writer=writer, encoding=encoding
        )

    def send_signal(self, sig) -> None:
        """
        Safely send signal according to subprocess.Popen.send_signal.
        """
        self.poll()
        if self.returncode is not None:
            return

        # Try to kill both progress and progress group in Unix.
        try:
            os.kill(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

        try:
            os.killpg(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


#
# Pexpect Executor
#

pexpect_executor_registry = Registry[Type["PexpectExecutor"]](
    "pexpect_executor_registry"
)


@pexpect_executor_registry(key="default")
class PexpectExecutor(Executor):
    """
    Executor using pexpect.

    Availability: Unix.
    """

    readonly_attr__ = ("reader", "encoding", "echo")

    def __init__(
        self,
        pexpect_params: FuncParams,
        reader: "PexpectReader",
        encoding: Union[str, None] = None,
        echo: bool = False,
    ) -> None:
        Executor.__init__(self)
        self.pexpect_params = pexpect_params
        self.reader = reader
        self.encoding = encoding or config.cmd_pipe_encoding
        self.echo = echo
        # Bind Reader.
        self.reader.bind(self)

    @property
    def pid(self):
        return getattr(self.process, "pid", None)

    def start(self) -> None:
        from pexpect import spawn

        self.process: Union[Missing, spawn] = spawn(
            *self.pexpect_params.args, **self.pexpect_params.kwargs
        )
        try:
            # NOTE: Set echo here rather than in the ``__init__``
            # method, because ``setecho`` may fail and raise exception.
            self.process.setecho(self.echo)
        except Exception as e:
            logger.error(str(e), stack_info=True)

    def read(self, timeout: float) -> str:
        return self.reader.read(timeout)

    def read_all(self) -> str:
        return self.reader.read_all()

    def readable(self) -> bool:
        return True

    def write(self, content: str) -> bool:
        try:
            self.process.send(content.encode(self.encoding))
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        else:
            return True

    def write_line(self, content: str) -> bool:
        try:
            self.process.sendline(content.encode(self.encoding))
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        else:
            return True

    def writable(self) -> bool:
        return True

    def keyboard_interrupt(self) -> None:
        self.process.kill(signal.SIGINT)
        if self.writable():
            # Send ``Ctrl-C`` to the pty.
            self.write(get_control_char("c"))

    def terminate(self) -> None:
        self.process.kill(signal.SIGTERM)

    def kill(self) -> None:
        if hasattr(signal, "SIGKILL"):
            self.process.kill(signal.SIGKILL)
        else:
            self.terminate()

    def is_running(self) -> bool:
        return self.is_started() and self.process.isalive()

    def __str__(self) -> str:
        return (
            f"{resolve_classname(self)}"
            f"(reader={str(self.reader)}, pid={self.pid}, "
            f"encoding={self.encoding})"
        )

    def __exit__(self, *args, **kwargs):
        self.process.close()
        return super().__exit__(*args, **kwargs)
