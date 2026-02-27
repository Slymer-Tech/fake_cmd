"""
Communication utils.
"""
import os
import json
import time
import uuid
import random
from threading import RLock
from abc import ABC, abstractmethod, ABCMeta
from slime_core.utils.base import (
    BaseList
)
from slime_core.utils.metabase import (
    ReadonlyAttr,
    _ReadonlyAttrMetaclass
)
from slime_core.utils.metaclass import (
    Metaclasses
)
from slime_core.utils.typing import (
    Dict,
    Any,
    Union,
    Missing,
    MISSING,
    List,
    Literal,
    Iterable,
    Stop,
    STOP
)
from . import config
from .common import LessThanAnything, polling, timeout_loop, uuid_base36, xor__
from .exception import retry_deco
from .logging import logger
from .file import (
    remove_file_with_retry,
    SINGLE_WRITER_LOCK_FILE_EXTENSION
)

#
# Messages.
#

class Message(ReadonlyAttr):
    """
    A message object.
    """
    readonly_attr__ = (
        'session_id',
        'msg_id',
        'timestamp',
        'content',
        'type'
    )
    
    json_attrs = (
        'session_id',
        'type',
        'content',
        'timestamp',
        'msg_id'
    )
    # Separator to sep the send fname components. Used 
    # for file sorting.
    send_fname_sep = '__'
    
    def __init__(
        self,
        *,
        session_id: str,
        type: str,
        content: Union[dict, None] = None,
        timestamp: Union[float, None] = None,
        msg_id: Union[str, None] = None
    ) -> None:
        """
        - ``session_id``: The connection session id.
        - ``type``: The message type (for different actions).
        - ``content``: The message content.
        - ``timestamp``: Time when the message is created.
        - ``msg_id``: A unique message id.
        """
        self.session_id = session_id
        self.type = type
        if (
            content is not None and 
            not isinstance(content, dict)
        ):
            logger.warning(
                f'Message content can only be ``dict`` or ``None``, not {str(content)}.'
            )
            content = {}
        self.content = content
        self.timestamp = timestamp or time.time()
        self.msg_id = msg_id or uuid_base36(uuid.uuid4().int)
    
    @property
    def confirm_fname(self) -> str:
        """
        Message confirmation file name.
        """
        return f'{self.msg_id}.confirm'
    
    @property
    def send_fname(self) -> str:
        """
        Return the message send file name.
        """
        return (
            f'{str(self.timestamp)}{Message.send_fname_sep}'
            f'{self.msg_id}.msg'
        )
    
    @property
    def output_namespace(self) -> str:
        """
        System output redirect namespace.
        """
        return f'{self.msg_id}_output'
    
    def to_json(self) -> str:
        """
        Transfer to json str.
        """
        kwargs = {k:getattr(self, k, None) for k in self.json_attrs}
        return json.dumps(kwargs)
    
    @classmethod
    def from_json(cls, json_str: str):
        """
        Create a message object from json str.
        """
        kwargs: Dict[str, Any] = json.loads(json_str)
        return cls(**{k:kwargs.get(k, None) for k in cls.json_attrs})

    @classmethod
    def clone(cls, msg: "Message"):
        return cls.from_json(msg.to_json())

    def get_content_item(self, key: str, default: Any):
        """
        Get message content item with given key and default value.
        """
        if not self.content:
            return default
        return self.content.get(key, default)

    def __bool__(self) -> bool:
        return True


class CommandMessage(Message):
    """
    Create alias names of the message attributes for better understanding.
    """
    readonly_attr__ = ('interactive',)
    
    def __init__(
        self,
        *,
        session_id: str,
        type: str,
        content: Union[str, dict, list, None] = None,
        timestamp: Union[float, None] = None,
        msg_id: Union[str, None] = None
    ) -> None:
        super().__init__(
            session_id=session_id,
            type=type,
            content=content,
            timestamp=timestamp,
            msg_id=msg_id
        )
    
    @property
    def cmd_content(self) -> Union[str, None]:
        if (
            self.content is None or 
            'cmd' not in self.content
        ):
            return None
        return self.content['cmd']
    
    @property
    def cmd_id(self) -> str:
        return self.msg_id

    @property
    def interactive(self) -> bool:
        """
        Whether the command is executed in an interactive mode.
        """
        return self.get_content_item('interactive', False)
    
    @property
    def kill_disabled(self) -> bool:
        """
        Whether the command is disabled to kill using keyboard interrupt.
        """
        return self.get_content_item('kill_disabled', False)

#
# Directory cache helpers.
#

_DIR_CACHE_SENTINEL = '.cache_refresh'


def invalidate_dir_cache(directory: str) -> None:
    """
    Force the OS / network FS to refresh its directory-entry and
    negative-dentry caches by creating and immediately removing a
    sentinel file.  On network file systems (e.g. NFS) this
    invalidates cached READDIR and LOOKUP results; on local file
    systems the operation is harmless.
    """
    sentinel = os.path.join(directory, _DIR_CACHE_SENTINEL)
    try:
        fd = os.open(sentinel, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        os.close(fd)
        os.remove(sentinel)
    except Exception:
        pass

#
# File Handlers.
#

class SequenceFileHandler(
    ABC,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    Process files in a namespace with specified sorting method. 
    Best compatible with single-writer.
    """
    readonly_attr__ = (
        'namespace',
        'max_files'
    )
    
    def __init__(
        self,
        namespace: str,
        max_files: int
    ):
        self.namespace = namespace
        self.max_files = max_files
        # file path queue, for sequence read.
        self.fp_queue = BaseList[str]()
        self.fp_queue_lock = RLock()
        # read queue
        # Contain files that have been read to avoid 
        # repeated files.
        self.read_queue = BaseList[str]()
        self.read_queue_lock = RLock()
        self.read_queue_max_size = 100
        os.makedirs(namespace, exist_ok=True)
    
    def read_one(
        self,
        detect_new_files: bool = True
    ) -> Union[str, Literal[False]]:
        """
        Read one sequence file (if any).
        
        ``detect_new_files``: Whether to detect new files if 
        ``fp_queue`` is empty. If set to ``False``, then directly 
        return ``False`` if ``fp_queue`` is empty.
        """
        if not self.check_namespace():
            return False
        
        with self.fp_queue_lock, self.read_queue_lock:
            if len(self.fp_queue) == 0:
                if not detect_new_files:
                    # Directly return.
                    return False
                
                try:
                    self.detect_files()
                except Exception as e:
                    logger.error(str(e), stack_info=True)
                    return False
            
            if len(self.fp_queue) == 0:
                return False
            
            fp = self.fp_queue.pop(0)
            if not os.path.exists(fp):
                logger.warning(
                    f'Message file removed after sent: {fp}'
                )
                return False
            # Check repeated messages.
            if fp in self.read_queue:
                remove_file_with_retry(fp)
                return False
            
            with open(fp, 'r') as f:
                content = f.read()
                remove_file_with_retry(fp)
            
            if len(self.read_queue) >= self.read_queue_max_size:
                self.read_queue.pop(0)
            self.read_queue.append(fp)
            return content
    
    def read_all(
        self,
        timeout: Union[float, Missing] = MISSING
    ) -> str:
        """
        Read all the remaining content (until timeout).
        """
        content = ''
        detect_new_files = True
        start = time.time()
        while True:
            c = self.read_one(detect_new_files=detect_new_files)
            # NOTE: Use ``c is False`` rather than 
            # ``not c`` here, because some sequence 
            # files may contain empty content.
            if c is False:
                return content
            content += c
            stop = time.time()
            if (
                timeout is not MISSING and 
                (stop - start) > timeout
            ):
                # Set ``detect_new_files`` to ``False`` 
                # and only read from existing ``fp_queue``.
                detect_new_files = False
    
    def write(
        self,
        fname: str,
        content: str,
        exist_ok: bool = False,
        empty_ok: bool = False
    ) -> Union[bool, Missing, Stop]:
        """
        Safely write a file using atomic rename (tmp → target). 
        Return whether the writing operation succeeded.
        """
        if not self.check_namespace():
            return False
        
        # Optimization: Avoid writing empty content.
        if (
            not content and 
            not empty_ok
        ):
            return MISSING
        
        try:
            # Optimization: Avoid piling up too many files in 
            # the folder.
            num_files = len(os.listdir(self.namespace))
            if num_files >= self.max_files:
                return STOP
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        
        fp = self.get_fp(fname)
        if (
            os.path.exists(fp) and 
            not exist_ok
        ):
            return False
        
        try:
            tmp_fp = fp + '.tmp'
            with open(tmp_fp, 'w') as f:
                f.write(content)
            os.replace(tmp_fp, fp)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        
        return True
    
    def get_fp(self, fname: str) -> str:
        """
        Get full file path given ``fname``.
        """
        return os.path.join(self.namespace, fname)
    
    def detect_files(self) -> None:
        """
        Detect and sort new files in the namespace, and add them 
        to ``fp_queue``.
        """
        if not self.check_namespace():
            return
        
        invalidate_dir_cache(self.namespace)
        
        with self.fp_queue_lock:
            fname_iter = filter(
                lambda fname: (
                    not fname.endswith('.tmp') and
                    not fname.endswith(SINGLE_WRITER_LOCK_FILE_EXTENSION) and
                    fname != _DIR_CACHE_SENTINEL
                ),
                os.listdir(self.namespace)
            )
            fname_list = self.sort(fname_iter)
            self.fp_queue.extend(map(self.get_fp, fname_list))
    
    @abstractmethod
    def sort(self, fname_iter: Iterable[str]) -> List[str]:
        """
        Return the sorted sequence of ``fname_iter``.
        """
        pass
    
    def check_namespace(self, silent: bool = True) -> bool:
        """
        Check whether the namespace exists.
        """
        namespace_exists = os.path.exists(self.namespace)
        if not namespace_exists and not silent:
            logger.warning(
                f'Namespace "{self.namespace}" does not exists.'
            )
        return namespace_exists


class OutputFileHandler:
    """
    Single-file output handler for stdout/stderr content.
    
    Instead of creating many small files (one per output chunk) and
    relying on directory listing to detect them, this handler appends
    all output to a single file and tracks read offsets.
    
    I/O comparison (for N output chunks):
        Old: N file creates + N os.listdir + N file reads + N file deletes
        New: N appends to 1 file + N seeks/reads on 1 file
    
    NFS safety: relies on close-to-open consistency.  The server closes
    the file after each append; the client opens it fresh for each read.
    This guarantees the client always sees the latest data without
    attribute-cache delays.
    
    File-size control: when the file exceeds ``config.max_output_file_bytes``,
    the server truncates and overwrites.  The client detects truncation
    (file size < read offset) and resets automatically.
    """
    _CONTENT_FNAME = 'output.dat'
    
    def __init__(self, namespace: str):
        self.namespace = namespace
        os.makedirs(namespace, exist_ok=True)
        self._content_fp = os.path.join(namespace, self._CONTENT_FNAME)
        self._read_offset: int = 0
        self._decode_buffer: bytes = b''
        try:
            self._written_bytes: int = os.path.getsize(self._content_fp)
        except OSError:
            self._written_bytes: int = 0
    
    def write(self, content: str, exist_ok: bool = False) -> Union[bool, Missing]:
        """Append *content* to the single output file.
        
        When the file exceeds ``config.max_output_file_bytes`` the
        server truncates via atomic rename (write tmp → rename) so
        the client never sees a half-written/empty file.  The client
        detects truncation (file size < read offset) and resets.
        """
        if not content:
            return MISSING
        
        data = content.encode('utf-8')
        try:
            if self._written_bytes > config.max_output_file_bytes:
                tmp_fp = self._content_fp + '.truncating'
                with open(tmp_fp, 'wb') as f:
                    f.write(data)
                os.replace(tmp_fp, self._content_fp)
                self._written_bytes = len(data)
            else:
                with open(self._content_fp, 'ab') as f:
                    f.write(data)
                self._written_bytes += len(data)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        return True
    
    def print(self, content: str, exist_ok: bool = False):
        return self.write(f'{content}\n', exist_ok=exist_ok)
    
    def read_one(
        self,
        detect_new_files: bool = True
    ) -> Union[str, Literal[False]]:
        """
        Read all content appended since the last read and return it
        as a single string.  Returns ``False`` when no new data is
        available.
        
        ``detect_new_files``:  When ``False``, only flush previously
        buffered partial UTF-8 bytes (used by ``read_all`` after its
        timeout expires so it stops pulling new data from disk).
        """
        if not detect_new_files:
            if self._decode_buffer:
                text = self._decode_buffer.decode('utf-8', errors='replace')
                self._decode_buffer = b''
                return text if text else False
            return False
        
        try:
            with open(self._content_fp, 'rb') as f:
                # Detect server-side truncation: if the file is now
                # smaller than our read offset, the server has reset
                # the file.  Discard any stale decode buffer and
                # start reading from 0.
                file_size = f.seek(0, 2)
                if file_size < self._read_offset:
                    self._read_offset = 0
                    self._decode_buffer = b''
                f.seek(self._read_offset)
                data = f.read()
        except FileNotFoundError:
            return False
        except OSError as e:
            logger.error(str(e), stack_info=True)
            return False
        
        if not data:
            return False
        
        if self._decode_buffer:
            data = self._decode_buffer + data
            self._decode_buffer = b''
        
        try:
            text = data.decode('utf-8')
            self._read_offset += len(data)
        except UnicodeDecodeError as e:
            if e.start > 0:
                text = data[:e.start].decode('utf-8')
                self._decode_buffer = data[e.start:]
                self._read_offset += e.start
            else:
                self._decode_buffer = data
                return False
        
        return text if text else False
    
    def read_all(
        self,
        timeout: Union[float, Missing] = MISSING
    ) -> str:
        """Read all remaining content (until no more data or *timeout*)."""
        content = ''
        detect_new_files = True
        start = time.time()
        while True:
            c = self.read_one(detect_new_files=detect_new_files)
            if c is False:
                return content
            content += c
            if (
                timeout is not MISSING and
                (time.time() - start) > timeout
            ):
                detect_new_files = False
    
    def check_namespace(self, silent: bool = True) -> bool:
        return os.path.exists(self.namespace)


class MessageHandler(SequenceFileHandler):
    """
    MessageHandler that is responsible for safe message reading and 
    sending.
    """
    
    def __init__(
        self,
        namespace: str,
        max_retries: Union[int, Missing] = MISSING,
        wait_timeout: Union[float, Missing] = MISSING
    ) -> None:
        SequenceFileHandler.__init__(
            self,
            namespace=namespace,
            max_files=config.max_message_files
        )
        self.max_retries = (
            max_retries if 
            max_retries is not MISSING else 
            config.msg_send_retries
        )
        self.wait_timeout = (
            wait_timeout if 
            wait_timeout is not MISSING else 
            config.msg_confirm_wait_timeout
        )
    
    def listen(self):
        """
        Endlessly listen messages in blocking mode.
        """
        for _ in polling():
            msg = self.read_one()
            if msg:
                yield msg
    
    def read_one(self) -> Union[Message, Literal[False]]:
        """
        Pop a new message (if any). Return ``False`` if no new messages.
        """
        content = super().read_one()
        if not content:
            return False
        try:
            # If the json decode fails (mostly because of 
            # file read and written at the same time, causing 
            # file inconsistency), directly return ``False``. 
            # Because the message will be re-sent if no confirm 
            # file is created, the consistency is ensured.
            msg = Message.from_json(content)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        # Create a confirmation symbol.
        create_symbol(self.get_fp(msg.confirm_fname))
        return msg
    
    def write(self, msg: Message) -> bool:
        """
        Send msg to namespace. Retry when the confirm symbol is not received, 
        until ``max_retries`` times. Return whether the message is successfully 
        received.
        """
        if not self.check_namespace(silent=False):
            return False
        
        attempt = 0
        msg_json = msg.to_json()
        max_retries = self.max_retries
        send_fp = self.get_fp(msg.send_fname)
        confirm_fp = self.get_fp(msg.confirm_fname)
        while True:
            # Avoid sending the same message.
            if not os.path.exists(send_fp):
                super().write(
                    msg.send_fname,
                    msg_json,
                    exist_ok=False
                )
            
            if wait_symbol(
                confirm_fp,
                timeout=self.wait_timeout
            ):
                return True
            
            # If wait symbol is False, then retry.
            if attempt >= max_retries:
                logger.warning(
                    f'Message sent {attempt} times, but not responded. Expected confirm file: '
                    f'{confirm_fp}. Message content: {msg_json}.'
                )
                return False
            
            attempt += 1
            logger.warning(
                f'Retrying sending the message: {msg_json}'
            )
    
    def sort(self, fname_iter: Iterable[str]) -> List[str]:
        """
        Sort the message file names by timestamps.
        """
        def get_timestamp(fname: str) -> float:
            """
            Get timestamp from the file path.
            """
            return float(fname.split(Message.send_fname_sep)[0])
        
        def filter_valid_fname(fname: str) -> bool:
            """
            Only keep the valid fname.  Do NOT remove ``.confirm``
            files—they are confirmation symbols that the sender
            polls for and must remain on disk until consumed.
            """
            try:
                get_timestamp(fname)
            except Exception:
                if not fname.endswith('.confirm'):
                    remove_file_with_retry(self.get_fp(fname))
                return False
            else:
                return True
        
        return sorted(
            filter(filter_valid_fname, fname_iter),
            key=lambda fp: get_timestamp(fp)
        )


class MessageChannel:
    """
    Single-file message channel for one-to-one communication.

    Uses the same append + offset-tracking pattern as
    ``OutputFileHandler``, but serialises ``Message`` objects as
    newline-delimited JSON (one JSON object per line).

    I/O comparison with ``MessageHandler`` (for N messages):
        Old: N file creates + N listdir + N reads + N deletes + N confirms
        New: N appends to 1 file + N offset-based reads from 1 file

    Safety: relies on close-to-open consistency—writer closes the
    file after each append; reader opens it fresh for each read.
    ``json.dumps`` escapes literal newlines, so ``\\n`` in the file
    is always a reliable message boundary.
    """
    _CONTENT_FNAME = 'channel.dat'

    def __init__(self, namespace: str):
        self.namespace = namespace
        os.makedirs(namespace, exist_ok=True)
        self._content_fp = os.path.join(namespace, self._CONTENT_FNAME)
        self._read_offset: int = 0
        self._pending_messages: List[Message] = []
        try:
            self._written_bytes: int = os.path.getsize(self._content_fp)
        except OSError:
            self._written_bytes: int = 0

    # ------------------------------------------------------------------
    # Writer side
    # ------------------------------------------------------------------

    def write(self, msg: Message) -> bool:
        """Append *msg* as a single JSON line.

        When the file exceeds ``config.max_output_file_bytes`` the
        writer truncates via atomic rename so the reader never sees
        a half-written file.  The reader detects truncation
        (file size < read offset) and resets automatically.
        """
        data = (msg.to_json() + '\n').encode('utf-8')
        try:
            if self._written_bytes > config.max_output_file_bytes:
                tmp_fp = self._content_fp + '.truncating'
                with open(tmp_fp, 'wb') as f:
                    f.write(data)
                os.replace(tmp_fp, self._content_fp)
                self._written_bytes = len(data)
            else:
                with open(self._content_fp, 'ab') as f:
                    f.write(data)
                self._written_bytes += len(data)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        return True

    # ------------------------------------------------------------------
    # Reader side
    # ------------------------------------------------------------------

    def read_one(self) -> Union[Message, Literal[False]]:
        """Return the next unread message, or ``False`` if none."""
        if not self._pending_messages:
            self._fetch_messages()
        if self._pending_messages:
            return self._pending_messages.pop(0)
        return False

    def _fetch_messages(self) -> None:
        """Read new data from disk and buffer complete JSON lines."""
        try:
            with open(self._content_fp, 'rb') as f:
                file_size = f.seek(0, 2)
                if file_size < self._read_offset:
                    self._read_offset = 0
                f.seek(self._read_offset)
                data = f.read()
        except (FileNotFoundError, OSError):
            return

        if not data:
            return

        last_newline = data.rfind(b'\n')
        if last_newline == -1:
            return

        complete_data = data[:last_newline + 1]
        self._read_offset += len(complete_data)

        text = complete_data.decode('utf-8', errors='replace')
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                msg = Message.from_json(line)
                self._pending_messages.append(msg)
            except Exception as e:
                logger.error(
                    f'Failed to parse channel message: {e}',
                    stack_info=True
                )

    # ------------------------------------------------------------------
    # Common
    # ------------------------------------------------------------------

    def check_namespace(self, silent: bool = True) -> bool:
        namespace_exists = os.path.exists(self.namespace)
        if not namespace_exists and not silent:
            logger.warning(
                f'Namespace "{self.namespace}" does not exist.'
            )
        return namespace_exists

#
# Symbol operations.
#

@retry_deco(suppress_exc=Exception)
def create_symbol(fp: str):
    """
    Create a symbol file. If ``fp`` exists, then do nothing.
    Uses atomic rename so the file appears fully on NFS
    without a transient ``.writing`` lock file.
    """
    if os.path.exists(fp):
        return
    
    tmp_fp = fp + '.tmp'
    fd = os.open(tmp_fp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
    os.close(fd)
    os.replace(tmp_fp, fp)


def wait_symbol(
    fp: str,
    timeout: Union[float, Missing] = MISSING,
    wait_for_remove: bool = False
) -> bool:
    """
    Wait a symbol file. Return ``True`` if the symbol is created 
    before timeout, otherwise ``False``.
    
    ``wait_for_remove``: whether the function is used to wait for 
    removing the symbol or creating the symbol.
    """
    timeout = config.symbol_wait_timeout if timeout is MISSING else timeout
    parent_dir = os.path.dirname(fp)
    
    for _ in timeout_loop(
        timeout,
        interval=config.polling_interval
    ):
        invalidate_dir_cache(parent_dir)
        if xor__(
            os.path.exists(fp),
            wait_for_remove
        ):
            remove_symbol(fp)
            return True
    return False


def remove_symbol(fp: str) -> None:
    """
    Remove a symbol file.
    """
    remove_file_with_retry(fp)


def check_symbol(fp: str) -> bool:
    """
    Check if the symbol exists. Non-blocking form of ``wait_symbol``.
    """
    if os.path.exists(fp):
        remove_symbol(fp)
        return True
    else:
        return False

#
# Connection API.
#

class Connection(ABC):

    @abstractmethod
    def connect(self) -> bool:
        """
        Three-way handshake to connect.
        """
        pass

    @abstractmethod
    def disconnect(self, initiator: bool):
        """
        Four-way handshake to disconnect. ``initiator``: whether 
        the disconnection is initiated locally.
        """
        pass
    
    @abstractmethod
    def check_connection(self) -> bool:
        """
        Check the connection state, decide whether to exit 
        and perform corresponding exit operations.
        """
        pass

#
# Heartbeat services.
#

class Heartbeat(ReadonlyAttr):
    """
    Used heartbeat to confirm the connection is still alive.
    """
    readonly_attr__ = (
        'receive_fp',
        'send_fp',
        'min_interval',
        'max_interval',
        'timeout'
    )
    
    def __init__(
        self,
        receive_fp: str,
        send_fp: str
    ) -> None:
        self.receive_fp: str = receive_fp
        self.last_receive: Union[float, None] = None
        self.send_fp: str = send_fp
        # Initialized to ``LessThanAnything``.
        self.last_beat: Union[float, LessThanAnything] = LessThanAnything()
        self.min_interval: float = config.heartbeat_min_interval
        self.max_interval: float = config.heartbeat_max_interval
        self.timeout: float = config.heartbeat_timeout
        self.set_interval()
    
    def beat(self) -> bool:
        now = time.time()
        if self.last_receive is None:
            # Set ``last_receive`` to now if it is None, because 
            # if the heartbeat never receives and ``last_receive`` 
            # is always None, we will never know when the timeout 
            # reaches. Set here rather than in the ``__init__``, 
            # because there may be a period of time between the 
            # Heartbeat object creation and the first beat (although 
            # most of the time it is short).
            self.last_receive = now
        
        if self.last_beat > (now - self.interval):
            # If within the interval, do not check and directly 
            # return True.
            return True
        
        create_symbol(self.send_fp)
        received = check_symbol(self.receive_fp)
        if received:
            self.last_receive = now
        elif (now - self.last_receive) > self.timeout:
            logger.warning(
                f'Heartbeat time out at {self.receive_fp}.'
            )
            return False

        self.last_beat = now
        # Set a new random interval after beat.
        self.set_interval()
        return True
    
    def set_interval(self) -> float:
        """
        Set a random interval between min and max.
        """
        self.interval = random.uniform(
            self.min_interval,
            self.max_interval
        )
