import os
from functools import wraps
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Missing,
    MISSING,
    Union,
    Callable,
    Any,
    Tuple,
    Dict
)
from fake_cmd.utils.comm import Message, CommandMessage, create_symbol
from fake_cmd.utils.file import remove_dir_with_retry, remove_file_with_retry
from fake_cmd.utils.logging import logger

SESSION_ID_SUFFIX = '__fcmd_sess'


class ServerInfo(ReadonlyAttr):
    """
    Server information.
    """
    readonly_attr__ = (
        'address',
        'listen_fname',
        'listen_namespace'
    )
    
    def __init__(
        self,
        address: str
    ) -> None:
        self.address = address
        # Used for checking whether another server is listening at the 
        # same address.
        self.server_check_fname = 'server.listen'
        self.server_listen_namespace = self.concat_server_path('server_listen')

    @property
    def server_check_fp(self) -> str:
        return self.concat_server_path(self.server_check_fname)

    def init_server(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.address, exist_ok=True)
        create_symbol(self.server_check_fp)
        
        for session_fname in filter(
            lambda fname: fname.endswith(SESSION_ID_SUFFIX),
            os.listdir(self.address)
        ):
            logger.info(f'Cleaning previous session cache: {session_fname}')
            remove_dir_with_retry(self.concat_server_path(session_fname))
    
    def clear_server(self):
        """
        Clear server listening files.
        """
        remove_dir_with_retry(self.server_listen_namespace)
        remove_file_with_retry(self.server_check_fp)
        try:
            # If empty, then remove.
            os.rmdir(self.address)
        except (OSError, FileNotFoundError):
            pass
    
    def concat_server_path(self, fname: str) -> str:
        return os.path.join(self.address, fname)


class SessionInfo(ServerInfo, ReadonlyAttr):
    """
    Parse session file names.
    """
    readonly_attr__ = (
        'address',
        'session_id'
    )
    
    def __init__(
        self,
        address: str,
        session_id: str
    ) -> None:
        ServerInfo.__init__(self, address)
        self.session_id = session_id
        # Message transfer.
        self.session_queue_namespace = self.concat_session_path('session_queue')
        self.client_queue_namespace = self.concat_session_path('client_queue')
        # Connect creation.
        self.conn_session_fp = self.concat_session_path('session.conn')
        self.conn_client_fp = self.concat_session_path('client.conn')
        # Disconnect.
        self.disconn_session_fp = self.concat_session_path('session.disconn')
        self.disconn_confirm_to_session_fp = self.concat_session_path('session.disconn.confirm')
        self.disconn_client_fp = self.concat_session_path('client.disconn')
        self.disconn_confirm_to_client_fp = self.concat_session_path('client.disconn.confirm')
        # Heartbeat.
        self.heartbeat_session_fp = self.concat_session_path('session.heartbeat')
        self.heartbeat_client_fp = self.concat_session_path('client.heartbeat')
        # Destroy signal (file-based, for cross-process signaling).
        self.destroy_session_fp = self.concat_session_path('session.destroy')
    
    @property
    def session_namespace(self) -> str:
        return os.path.join(self.address, self.session_id)
    
    def init_server(self):
        logger.warning(
            f'``init_server`` should not be called in the ``SessionInfo``.'
        )
    
    def clear_server(self):
        logger.warning(
            f'``clear_server`` should not be called in the ``SessionInfo``.'
        )
    
    def init_session(self):
        """
        Create corresponding files. Should only be called once by session.
        """
        os.makedirs(self.session_namespace, exist_ok=True)
        os.makedirs(self.session_queue_namespace, exist_ok=True)
        os.makedirs(self.client_queue_namespace, exist_ok=True)
    
    def clear_session(self):
        """
        Clear the session files.
        """
        remove_dir_with_retry(self.session_namespace)
    
    def concat_session_path(self, fname: str) -> str:
        return os.path.join(self.session_namespace, fname)
    
    def message_output_namespace(self, msg: Message) -> str:
        """
        Get the message output namespace.
        """
        return self.concat_session_path(msg.output_namespace)
    
    def command_terminate_confirm_fp(self, msg: CommandMessage) -> str:
        """
        Get the command terminate confirm fp.
        """
        return self.concat_session_path(f'{msg.cmd_id}.term')

#
# Action Utils.
#

ActionFunc = Callable[[Any, Message], Union[None, Tuple[str, ...]]]


def dispatch_action(
    registry: Registry[ActionFunc],
    type: str,
    caller: str
) -> Union[ActionFunc, Missing]:
    """
    Dispatch the cations with given msg types.
    """
    action = registry.get(type, MISSING)
    if action is MISSING:
        logger.warning(
            f'Unsupported message type received in {caller}: {type}, '
            f'and it is ignored. Supported types: {tuple(registry.keys())}'
        )
    return action


def param_check(
    required: Tuple[str, ...]
):
    """
    Check the required message content params. This function forces 
    the message content to be a dict.
    """
    def decorator(func: ActionFunc) -> ActionFunc:
        @wraps(func)
        def wrapper(self, msg: Message) -> Union[None, Tuple[str, ...]]:
            content: Dict = msg.content
            if content is None:
                return required
            
            missing_params = tuple(
                filter(lambda param: param not in content, required)
            )
            if len(missing_params) > 0:
                return missing_params
            
            return func(self, msg)
        return wrapper
    return decorator
