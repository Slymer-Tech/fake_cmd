import os
import re
import selectors
import subprocess
from slime_core.utils.registry import Registry
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.typing import (
    Type,
    Union,
    Missing,
    MISSING
)
from fake_cmd.utils.common import timeout_loop, resolve_classname
from fake_cmd.utils.logging import logger
from . import ExecutorComponent, PlatformPopenExecutor, PexpectExecutor

#
# Stream bytes service.
#

class StreamBytesParser(ReadonlyAttr):
    """
    Parse bytes stream eagerly.
    """
    readonly_attr__ = ('encoding',)

    def __init__(
        self,
        encoding: str = 'utf-8'
    ) -> None:
        self.buffer = b''
        self.encoding = encoding

    def parse(self, data: bytes) -> str:
        """
        Try to parse new data with previous stream buffer.
        """
        if self.buffer:
            data = self.buffer + data

        parsed = ''
        try:
            parsed = data.decode(encoding=self.encoding)
        except UnicodeDecodeError as e:
            if e.start != 0:
                # Parse the previous right data.
                parsed = data[:e.start].decode(encoding=self.encoding)
                self.buffer = data[e.start:]
            elif e.end != len(data):
                # This means there is some error in the middle, 
                # then directly parse the decoded str with error 
                # replace (to explicitly show that there is an 
                # error).
                parsed = data.decode(encoding=self.encoding, errors='replace')
                self.buffer = b''
            else:
                # The total bytes are decoded with error, should wait 
                # the following bytes.
                self.buffer = data
        else:
            # Successfully decode, clear the buffer.
            self.buffer = b''
        return parsed

#
# Popen Readers
#

popen_reader_registry = Registry[Type["PopenReader"]]('popen_reader_registry')


@popen_reader_registry(key='none')
class PopenReader(ExecutorComponent[PlatformPopenExecutor]):
    """
    Interface to read output of the popen process.
    """
    
    readonly_attr__ = ('executor',)
    
    def __init__(self) -> None:
        ExecutorComponent.__init__(self)
    
    @property
    def encoding(self) -> str:
        return self.executor.encoding
    
    def read_init(self) -> None:
        """
        This method should be called when the executor starts, 
        and it notifies the reader that the Popen object has 
        been created and the stdout as well as stderr can be 
        accessed.
        """
        pass
    
    def read(self, timeout: float) -> str:
        """
        Read the content within the timeout. This method 
        concat the results of ``_selector_read_one`` within 
        the timeout and return.
        """
        return ''
    
    def read_all(self) -> str:
        """
        Read all the content util file end. Make sure the 
        process is end when you call ``read_all``, else the 
        read will be blocked.
        """
        return ''
    
    def readable(self) -> bool:
        """
        Return whether the reader is readable. Default to True.
        """
        return True
    
    def get_popen_stdout(self):
        """
        Get the stdout argument passed to ``Popen``.
        """
        return None
    
    def get_popen_stderr(self):
        """
        Get the stderr argument passed to ``Popen``.
        """
        return None
    
    def __str__(self) -> str:
        return f'{resolve_classname(self)}()'


@popen_reader_registry(key='default')
class PipePopenReader(PopenReader):
    """
    Non-blocking read stdout pipes within timeout.
    """
    readonly_attr__ = (
        'selector',
        'bytes_parser'
    )

    def __init__(self) -> None:
        PopenReader.__init__(self)
        # NOTE: Use ``selector`` to get non-blocking outputs.
        # TODO: If ``SessionCommand`` changes to a ``Process``, then the 
        # ``DefaultSelector`` should be created in the ``read_init`` method 
        # (which runs in the subprocess).
        self.selector = selectors.DefaultSelector()
        self.bytes_parser: Union[StreamBytesParser, Missing] = MISSING
    
    def bind(self, executor: PlatformPopenExecutor) -> None:
        super().bind(executor)
        # The encoding attribute will be available after binding, so the 
        # ``bytes_parser`` object should be created here.
        self.bytes_parser = StreamBytesParser(encoding=self.encoding)

    def read_init(self) -> None:
        self.selector.register(self.executor.stdout, selectors.EVENT_READ)

    def read(self, timeout: float) -> str:
        content = ''
        for _ in timeout_loop(timeout):
            chunk = self._selector_read_one()
            if chunk:
                content += chunk
            elif content:
                break
        return content

    def read_all(self) -> str:
        # Non-blocking mode.
        stdout = self.executor.stdout
        for key, _ in self.selector.select(0):
            if key.fileobj == stdout:
                # If ready, read all and return.
                try:
                    stdout.flush()
                except Exception as e:
                    logger.error(str(e), stack_info=True)
                return self.bytes_parser.parse(stdout.read())
        # If not ready, directly return empty str.
        return ''

    def _selector_read_one(self) -> str:
        """
        Read available data from stdout if selector is ready.
        Uses os.read for non-blocking chunk reads instead of
        single-byte reads to drastically reduce syscall overhead.
        """
        stdout = self.executor.stdout
        for key, _ in self.selector.select(0):
            if key.fileobj == stdout:
                try:
                    data = os.read(stdout.fileno(), 65536)
                except OSError:
                    return ''
                if not data:
                    return ''
                return self.bytes_parser.parse(data)
        return ''
    
    def get_popen_stdout(self):
        return subprocess.PIPE
    
    def get_popen_stderr(self):
        return subprocess.STDOUT

#
# Pexpect Readers
#

pexpect_reader_registry = Registry[Type["PexpectReader"]]('pexpect_reader_registry')


class PexpectReader(ExecutorComponent[PexpectExecutor]):
    # NOTE: Unlike subprocess.Popen, which can specify the stdout and stderr to be 
    # None to redirect the output of the subprocess to the standard stream of the 
    # parent process, ``pexpect`` uses pty.fork to redirect the output to a file, 
    # which means we should always read the content to avoid the pipe piling up and 
    # causing deadlock, so the pexpect_reader_registry does not provide the 'none' 
    # option (which reads nothing in this mode) as ``PopenReader`` does.
    
    readonly_attr__ = ('bytes_parser',)
    
    def __init__(self) -> None:
        ExecutorComponent.__init__(self)
    
    @property
    def encoding(self) -> str:
        return self.executor.encoding
    
    def bind(self, executor: PexpectExecutor) -> None:
        super().bind(executor)
        # The encoding attribute will be available after binding, so the 
        # ``bytes_parser`` object should be created here.
        self.bytes_parser = StreamBytesParser(encoding=self.encoding)
    
    def read(self, timeout: float) -> str:
        """
        Read the content within the timeout.
        """
        return ''
    
    def read_all(self) -> str:
        """
        Read all the content util file end. Make sure the 
        process is end when you call ``read_all``, else the 
        read will be blocked.
        """
        return ''
    
    def readable(self) -> bool:
        """
        Return whether the reader is readable. Default to True.
        """
        return True
    
    def parse_content(self, content: Union[str, bytes, None]) -> str:
        """
        Parse the content to str according to different content types.
        """
        if isinstance(content, bytes):
            return self.bytes_parser.parse(content)
        elif isinstance(content, str):
            return content
        elif content is None:
            return ''
        else:
            logger.warning(
                f'Unsupported type found: {type(content)}'
            )
            return ''
    
    def __str__(self) -> str:
        return f'{resolve_classname(self)}()'


@pexpect_reader_registry(key='default')
class DefaultPexpectReader(PexpectReader):
    
    def read(self, timeout: float) -> str:
        content = ''
        for _ in timeout_loop(timeout):
            content += self._read_one()
        return content
    
    def read_all(self) -> str:
        process = self.executor.process
        content = process.read()
        return self.parse_content(content)
    
    def _read_one(self) -> str:
        """
        Read one char (if available).
        """
        import pexpect
        process = self.executor.process
        index = process.expect([
            # Expect any char.
            re.compile('.{1}'.encode(self.encoding), re.DOTALL),
            process.delimiter,
            pexpect.TIMEOUT
        ], timeout=0)
        if index == 0:
            content = process.after
        else:
            content = process.before
        return self.parse_content(content)
