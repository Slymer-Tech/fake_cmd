from slime_core.utils.typing import (
    Union,
    Tuple
)
from fake_cmd import __version__


class Config:
    
    def __init__(self) -> None:
        # Common polling interval
        self.polling_interval: float = 0.25
        # NOTE: The cmd is responsible for content output, 
        # so the polling interval should be short.
        self.cmd_polling_interval: float = 0.02
        # Read the subprocess pipe output within the timeout 
        # and return.
        # NOTE: This should be slightly larger than the 
        # ``cmd_polling_interval``, because this value determines 
        # how fast a new output file is created, and it should 
        # be slower than the client consuming the output files 
        # to avoid new files piling up in the long term.
        self.cmd_pipe_read_timeout: float = 0.03
        self.cmd_pipe_read_timeout_when_terminate: float = 0.5
        # Limit on the number of files (for MessageHandler).
        self.max_message_files: int = 1000
        self.max_output_files: int = 1000
        # Maximum size (bytes) of the single output file before the
        # server truncates it.  Client-read data is already displayed
        # and won't be needed again, so truncation only loses output
        # that the client hasn't fetched yet (similar to the old
        # ``max_output_files`` behaviour).  Default: 50 MiB.
        self.max_output_file_bytes: int = 50 * 1024 * 1024
        # command pipe output encoding method.
        self.cmd_pipe_encoding: str = 'utf-8'
        self.cmd_executable: Union[str, None] = None
        # When ``kill_cmd`` is sent or client terminated, 
        # read the remaining content util timeout.
        self.cmd_client_read_timeout: float = 1.0
        self.cmd_pool_schedule_interval: float = 0.2
        # Server shutdown wait timeout.
        self.server_shutdown_wait_timeout: float = 5.0
        # Common symbol timeouts.
        self.symbol_wait_timeout: float = 5.0
        self.symbol_remove_timeout: float = 0.01
        # Retries
        self.msg_send_retries: int = 3
        self.msg_confirm_wait_timeout: float = 3.0
        self.exception_retries: int = 3
        self.exception_wait_timeout: float = 0.5
        # Heartbeat settings.
        # Create or check symbol at a random interval between 
        # min and max, in order to reduce file conflicts.
        self.heartbeat_min_interval: float = 10.0
        self.heartbeat_max_interval: float = 20.0
        self.heartbeat_timeout: float = 600.0
        # The version info for compatibility check.
        self.version: Tuple[int, int, int] = tuple(int(v) for v in __version__.split('.')[:3])
        # For system settings.
        self.platform: str = 'unix'
        self.posix_shlex: bool = True


config = Config()


def version_check(
    version: Union[Tuple[int, int, int], None],
    *,
    min_version: Union[Tuple[int, int, int], None] = None,
    max_version: Union[Tuple[int, int, int], None] = None,
    verbose: bool = True
) -> bool:
    """
    Check whether the server version is compatible with some client
    APIs.
    """
    check_passed = False
    if version is None:
        check_passed = False
    else:
        version = tuple(version)
        check_passed = (
            ((not min_version) or version >= tuple(min_version)) and 
            ((not max_version) or version <= tuple(max_version))
        )
    
    if not check_passed and verbose:
        from .logging import logger
        logger.warning(
            f'Version check failed, expected version: '
            f'min: {min_version}, max: {max_version}, actual '
            f'version: {version}. You may need to get the '
            'latest updates.'
        )
    return check_passed
