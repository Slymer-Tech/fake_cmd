import os
import subprocess
from slime_core.utils.registry import Registry
from slime_core.utils.typing import Type
from fake_cmd.utils.common import resolve_classname
from fake_cmd.utils.logging import logger
from . import ExecutorComponent, PlatformPopenExecutor

#
# Popen Writers
# NOTE: The default popen writer does not specify any input method, and fake_cmd
# cannot write input to Popen in this mode.
#

popen_writer_registry = Registry[Type["PopenWriter"]]("popen_writer_registry")


@popen_writer_registry(key="default")
class PopenWriter(ExecutorComponent[PlatformPopenExecutor]):
    """
    Interface to write user input to the popen process.
    """

    readonly_attr__ = ("executor",)

    def __init__(self) -> None:
        ExecutorComponent.__init__(self)

    @property
    def encoding(self) -> str:
        return self.executor.encoding

    def write(self, content: str) -> bool:
        return False

    def writable(self) -> bool:
        return False

    def get_popen_stdin(self):
        """
        Get the stdin argument passed to ``Popen``.
        """
        return None

    def close(self):
        """
        Close the stdin.
        """
        pass

    def __str__(self) -> str:
        return f"{resolve_classname(self)}()"


@popen_writer_registry(key="pipe")
class PipePopenWriter(PopenWriter):
    """
    Write input to the pipe.
    """

    def write(self, content: str) -> bool:
        popen_stdin = self.executor.stdin
        if not popen_stdin:
            logger.warning("Popen stdin pipe is NoneOrNothing.")
            return False

        try:
            popen_stdin.write(content.encode(self.encoding))
            popen_stdin.flush()
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        else:
            return True

    def writable(self) -> bool:
        return True

    def get_popen_stdin(self):
        return subprocess.PIPE

    def close(self):
        # Do nothing here, because the subprocess
        # will automatically close the stdin on
        # ``__exit__``.
        return

    def __str__(self) -> str:
        popen_stdin = self.executor.stdin
        fileno = popen_stdin.fileno() if popen_stdin else None
        return (
            f"{resolve_classname(self)}"
            f"(stdin={str(popen_stdin)}, "
            f"stdin_fileno={str(fileno)})"
        )


@popen_writer_registry(key="pty")
class PtyPopenWriter(PopenWriter):
    """
    Write input through pty master and slave.

    Availability: Unix.

    NOTE: The popen writer using ``pty.openpty`` is not an encouraged
    behavior, and if ``kill_disabled`` is set to True in the interactive
    mode, the PtyWriter may cause the process unable to quit because of the
    unclosed master and slave files. Using ``pexpect`` is encouraged.
    """

    readonly_attr__ = ("master", "slave")

    def __init__(self) -> None:
        super().__init__()
        import pty

        self.master, self.slave = pty.openpty()

    def write(self, content: str) -> bool:
        try:
            os.write(self.master, content.encode(self.encoding))
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        else:
            return True

    def writable(self) -> bool:
        return True

    def get_popen_stdin(self):
        return self.slave

    def close(self):
        # Should manually close the pty.
        try:
            os.close(self.master)
        except Exception as e:
            logger.error(str(e), stack_info=True)

        try:
            os.close(self.slave)
        except Exception as e:
            logger.error(str(e), stack_info=True)

    def __str__(self) -> str:
        return (
            f"{resolve_classname(self)}" f"(master={self.master}, slave={self.slave})"
        )
