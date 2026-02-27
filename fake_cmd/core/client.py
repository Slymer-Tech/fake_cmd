import os
import sys
import uuid
from functools import wraps
from contextlib import contextmanager
from threading import Thread, Event, RLock
from abc import ABCMeta
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import (
    Metaclasses,
    _ReadonlyAttrMetaclass
)
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    NOTHING,
    MISSING,
    Union,
    Callable,
    Any,
    Literal,
    Missing,
    Tuple,
    Iterable
)
from fake_cmd.utils import (
    config,
    version_check
)
from fake_cmd.utils.comm import (
    Connection,
    Message,
    wait_symbol,
    create_symbol,
    check_symbol,
    Heartbeat,
    CommandMessage,
    MessageHandler,
    MessageChannel,
    OutputFileHandler
)
from fake_cmd.utils.common import get_server_name, polling, uuid_base36
from fake_cmd.utils.file import (
    remove_file_with_retry,
    remove_dir_with_retry
)
from fake_cmd.utils.exception import CLITerminate, ignore_keyboard_interrupt
from fake_cmd.utils.parallel import (
    LifecycleRun,
    ExitCallbackFunc
)
from fake_cmd.utils.cmd_arg import (
    cmd_parser_registry,
    CMD_SEPARATOR,
    CMDSplit,
    split_cmd
)
from fake_cmd.utils.logging import logger
from . import SessionInfo, ActionFunc, dispatch_action, param_check, SESSION_ID_SUFFIX

# Keyboard interrupt count corresponding to different meanings.
CMD_INTERRUPT = 1
CMD_TERMINATE = 2
CMD_KILL = 3
CMD_BACKGROUND = 4
# Document Separator length
DOC_SEP_LEN = 50

CLIENT_HELP = f"""
{'=' * DOC_SEP_LEN}
Help Document [fake_cmd]:

1. Inner Commands:
>>> ``help``: Get the help document.
>>> ``exit``: Shutdown the client and disconnect session.
>>> ``sid``: Get the session id of the client.
>>> ``ls-session``: List all the alive sessions.
>>> ``ls-cmd``: List all the commands executing or queued.
>>> ``ls-back``: List the background command of the current session.
>>> ``ls-server-version``: Show the server version (for compatibility check).
>>> ``version_strict_on``: Set the version strict to True.
>>> ``version_strict_off``: Set the version strict to False.

2. Advanced Running:
=> Required Syntax: [Command options (as follows)]{CMD_SEPARATOR}[Your real command]
=> Example: inter --exec /bin/bash -- python -i
(In the above example, ``inter --exec /bin/bash`` is the command options, and ``python -i`` \
is your real command to run on the server)
=> Supported command options:
>>> ``cmd``: Run the command with advanced options. Use ``cmd -h`` to get more help.
>>> ``inter``: Run the command in the interactive mode. Input is enabled. Use \
``inter -h`` to get more help.
>>> ``pexpect``: Run the command in an advanced interactive mode using the additional \
package ``pexpect`` (Availability: Unix). Use ``pexpect -h`` to get more help.

3. Killing the Running Command:
>>> If using common ``cmd`` or ``--kill_disabled`` is not specified in the interactive \
mode, the number of times ``Ctrl+C`` is pressed represents different behaviors:
- ({CMD_INTERRUPT}): Send keyboard interrupt to the command.
- ({CMD_TERMINATE}): Terminate the command.
- ({CMD_KILL}): Kill the command.
- (>={CMD_BACKGROUND}): Put the command to the background (and then you can use \
``ls-back`` to check it, or manually use ``kill`` to kill it).
>>> If ``--kill_disabled`` is specified in the interactive mode, one ``Ctrl+C`` corresponds \
to one keyboard interrupt sent to the command, and you may need to manually exit the \
command according to different commands (e.g., use ``exit()`` in the Python interactive \
mode, and use ``exit`` in the /bin/bash, etc.).

4. Danger Zone:
>>> ``server_shutdown``: Shutdown the whole server. NOTE: The server can be shutdown by any \
of the Clients, without any authorization required. When shutdown is executed, the Server will \
terminate all the commands and destroy all the sessions. BE CAREFUL TO USE IT!!!
{'=' * DOC_SEP_LEN}
"""


class State(ReadonlyAttr):
    """
    Communication items between client and cli.
    """
    readonly_attr__ = (
        'connected',
        'connection_failed',
        'cmd_running',
        'cmd_running_lock',
        'cmd_terminate_remote',
        'keyboard_interrupt_lock',
        'keyboard_interrupt_max_cnt',
        'cmd_finished',
        'terminate',
        'terminate_lock',
        'unable_to_communicate',
        'cli_exit'
    )
    
    def __init__(self) -> None:
        # Connection-related states.
        self.connected = Event()
        self.connection_failed = Event()
        # Indicator that whether a command is running.
        self.cmd_running = Event()
        self.cmd_running_lock = RLock()
        # Command terminated from remote.
        self.cmd_terminate_remote = Event()
        # Counting the "KeyboardInterrupt" Events.
        self.keyboard_interrupt_lock = RLock()
        self.keyboard_interrupt = 0
        # NOTE: Avoid endless Keyboard Interrupt from a 
        # process, which may cause integer overflow.
        self.keyboard_interrupt_max_cnt = 10
        # Command finished.
        self.cmd_finished = Event()
        # CLI terminate indicator.
        self.terminate = Event()
        self.terminate_lock = RLock()
        # Unable to communicate to server, so in 
        # ``clear_cache``, it will clear the namespace 
        # ignoring whether the server has already 
        # finished reading messages.
        self.unable_to_communicate = Event()
        # Set the cli exit to notify the Client to disconnect.
        self.cli_exit = Event()
    
    def reset(self) -> None:
        self.cmd_terminate_remote.clear()
        # Reset the keyboard interrupt.
        self.reset_keyboard_interrupt()
        self.cmd_finished.clear()
        self.set_cmd_running(False)
    
    @contextmanager
    def reset_ctx(self):
        """
        State reset context manager.
        """
        self.reset()
        try:
            yield
        finally:
            self.reset()

    def add_keyboard_interrupt(self, cnt: int = 1) -> None:
        with self.keyboard_interrupt_lock:
            if (
                self.keyboard_interrupt >= self.keyboard_interrupt_max_cnt
            ):
                return
            self.keyboard_interrupt += cnt
    
    def reset_keyboard_interrupt(self) -> None:
        with self.keyboard_interrupt_lock:
            self.keyboard_interrupt = 0

    def get_keyboard_interrupt(self) -> int:
        with self.keyboard_interrupt_lock:
            return self.keyboard_interrupt
    
    def set_cmd_running(self, running: bool) -> None:
        with self.cmd_running_lock:
            if running:
                self.cmd_running.set()
            else:
                self.cmd_running.clear()


def cli_action_version_check(
    min_version: Union[Tuple[int, int, int], None] = None,
    max_version: Union[Tuple[int, int, int], None] = None,
    verbose: bool = True
):
    def decorator(func: Callable[..., Union["ClientCommand", Literal[False]]]):
        @wraps(func)
        def wrapper(self: "CLI", *args, **kwargs) -> Union["ClientCommand", Literal[False]]:
            if (
                self.version_strict and 
                not version_check(
                    version=self.server_version,
                    min_version=min_version,
                    max_version=max_version,
                    verbose=verbose
                )
            ):
                return False
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


CLIActionFunc = Callable[[Any, CMDSplit], Union["ClientCommand", Literal[False]]]


class CLI(
    LifecycleRun,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    The CLI starts the Client and Command, because only the main 
    process can capture keyboard interrupts, and it is useful to 
    interrupt the input() function. In older versions, the Client 
    is responsible for starting the CLI, and because of the above 
    reason, now it is reversed.
    """
    readonly_attr__ = (
        'client',
        'current_cmd_lock',
        'server_name',
        'cmd_parser',
        'inter_cmd_parser'
    )
    
    cli_registry = Registry[CLIActionFunc]('cli_registry')
    
    def __init__(
        self,
        address: str,
        id_prefix: Union[str, None] = None
    ):
        LifecycleRun.__init__(self)
        self.client = Client(self, address, id_prefix)
        self.current_cmd: Union["ClientCommand", None] = None
        # Use a current cmd lock to make it consistent with 
        # the current waiting server cmd.
        self.current_cmd_lock = RLock()
        self.server_name = get_server_name(self.session_info.address)
    
    @property
    def session_info(self) -> SessionInfo:
        return self.client.session_info
    
    @property
    def state(self) -> State:
        return self.client.state
    
    @property
    def input_hint(self) -> str:
        return f'[fake_cmd {self.server_name}]$ '
    
    @property
    def session_writer(self) -> MessageHandler:
        return self.client.session_writer
    
    @property
    def send_msg_to_session(self):
        return self.client.send_msg_to_session
    
    @property
    def send_msg_to_server(self):
        return self.client.send_msg_to_server
    
    @property
    def server_version(self) -> Union[Tuple[int, int, int], None]:
        return self.client.server_version
    
    @property
    def version_strict(self) -> bool:
        return self.client.version_strict
    
    def set_current_cmd(self, cmd: Union["ClientCommand", None]) -> None:
        with self.current_cmd_lock:
            self.current_cmd = cmd
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        state = self.state
        self.client.start()
        for _ in polling():
            if state.connection_failed.is_set():
                return False
            elif state.connected.is_set():
                return True
    
    def running(self):
        state = self.state
        
        def safely_check_terminate() -> bool:
            """
            Safely check whether to terminate.
            """
            # Use double check to make it safe.
            if state.terminate.is_set():
                return True
            with state.terminate_lock:
                if state.terminate.is_set():
                    return True
            return False
        
        while True:
            try:
                # Command input.
                with state.reset_ctx():
                    # The terminate state should be checked before and after the 
                    # cmd input.
                    if safely_check_terminate(): return
                    try:
                        cmd_str = input(self.input_hint)
                    except KeyboardInterrupt:
                        print('Keyboard Interrupt (CLI Input).')
                        continue
                    except EOFError:
                        print('EOF (CLI Input).')
                        continue
                    finally:
                        if safely_check_terminate(): return
                
                # Process the command.
                with state.reset_ctx():
                    # NOTE: the command has already been sent to the server here.
                    cmd = self.process_cmd(cmd_str)
                    if cmd:
                        self.wait_cmd(cmd)
            except CLITerminate:
                print('CLI exiting...')
                return
            except KeyboardInterrupt:
                print('Keyboard Interrupt (CLI).')
            except BaseException as e:
                logger.error(str(e), stack_info=True)
    
    def after_running(self, *args):
        # Wait the Client to quit.
        self.state.cli_exit.set()
        self.client.join()
    
    #
    # Command operations.
    #
    
    def process_cmd(self, cmd_str: Union[str, None]) -> Union["ClientCommand", Literal[False]]:
        """
        Process the input cmd. Return a ``Command`` object if the cmd should 
        be executed in the server and it is successfully received, else return 
        ``False``.
        """
        if not cmd_str:
            return False
        
        cmd_splits = split_cmd(cmd_str)
        if not cmd_splits:
            return False
        
        possible_options, _ = cmd_splits
        if possible_options is MISSING:
            cli_func = MISSING
        else:
            cli_func = self.cli_registry.get(possible_options[0], MISSING)
        if cli_func is MISSING:
            # Parse default args.
            cmd_parser = cmd_parser_registry.get('cmd')()
            args = cmd_parser.parse_args(
                args=[],
                strict=False
            )
            if args is MISSING:
                return False
            
            # The whole ``cmd_str`` will be seen as a command.
            args.cmd = cmd_str
            return self.send_session_cmd(
                content=cmd_parser.make_cmd_content(args),
                type='cmd'
            )
        else:
            return cli_func(self, cmd_splits)
    
    # NOTE: Use version check here. Since (0, 0, 1), the msg 
    # content is either a dict or None, and str is no longer 
    # supported.
    @cli_action_version_check(min_version=(0, 0, 1))
    def send_session_cmd(
        self,
        content: dict,
        type: str = 'cmd'
    ) -> Union["ClientCommand", Literal[False]]:
        """
        Send the command to the session.
        """
        session_info = self.session_info
        msg = CommandMessage(
            session_id=session_info.session_id,
            type=type,
            content=content
        )
        
        def cmd_exit(*args):
            self.set_current_cmd(None)
        
        cmd = ClientCommand(cli=self, msg=msg, exit_callbacks=[cmd_exit])
        self.set_current_cmd(cmd)
        with ignore_keyboard_interrupt():
            # NOTE: This process should not be interrupted.
            res = self.session_writer.write(msg)
            if not res:
                self.set_current_cmd(None)
                return False
            return cmd
    
    def wait_cmd(self, cmd: "ClientCommand"):
        """
        Wait until the command quit.
        """
        with ignore_keyboard_interrupt():
            # NOTE: The keyboard interrupt should be 
            # ignored here.
            state = self.state
            cmd_input = cmd.cmd_input
            interactive = cmd.interactive
            # Set ``cmd_running`` here in advance, because the 
            # command thread may not have set the ``cmd_running`` 
            # Event before the below ``while True`` block starts 
            # running, and the ``while True`` will return if 
            # ``cmd_running`` is not set.
            state.set_cmd_running(True)
            cmd.start()
        while True:
            try:
                if not cmd.safely_check_cmd_running():
                    return
                if interactive:
                    # NOTE: Keyboard Interrupt may not be 
                    # caught in the ``run`` function (because 
                    # the interrupt may be raised before the 
                    # try-except clause), so it should be put 
                    # in the outside try-except clause to keep 
                    # it safe.
                    cmd_input.run()
                for _ in polling(config.cmd_polling_interval):
                    # NOTE: In higher versions of Python (e.g., Python 3.10), 
                    # thread.is_alive will return False and thread.join will 
                    # directly return after a keyboard interrupt event is raised. 
                    # However, the thread is still running, causing inconsistency. 
                    # So we further check whether ``cmd_running`` Event is cleared 
                    # to make sure that the ``cmd`` thread has finished.
                    # This bug will appear at least in macOS.
                    if not cmd.safely_check_cmd_running():
                        return
            except KeyboardInterrupt:
                with ignore_keyboard_interrupt():
                    print('Keyboard Interrupt (CLI Command).')
                    state.add_keyboard_interrupt()
    
    #
    # Actions.
    #
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd_splits: CMDSplit):
        raise CLITerminate
    
    @cli_registry(key='sid')
    def cli_sid(self, cmd_splits: CMDSplit) -> Literal[False]:
        print(
            f'Session id: {self.session_info.session_id}'
        )
        return False
    
    @cli_registry(key='help')
    def cli_help(self, cmd_splits: CMDSplit) -> Literal[False]:
        print(CLIENT_HELP.strip('\n'))
        return False
    
    @cli_registry(key='ls-server-version')
    def ls_server_version(self, cmd_splits: CMDSplit) -> Literal[False]:
        logger.info(
            f'Server version: {self.server_version}'
        )
        return False
    
    @cli_registry(key='ls-client-version')
    def ls_server_version(self, cmd_splits: CMDSplit) -> Literal[False]:
        logger.info(
            f'Client version: {config.version}'
        )
        return False
    
    @cli_registry(key='version_strict_on')
    def version_strict_on(self, cmd_splits: CMDSplit) -> Literal[False]:
        self.client.version_strict = True
        logger.info('Set ``version_strict`` to ``True``.')
        return False
    
    @cli_registry(key='version_strict_off')
    def version_strict_off(self, cmd_splits: CMDSplit) -> Literal[False]:
        self.client.version_strict = False
        logger.info('Set ``version_strict`` to ``False``.')
        return False
    
    @cli_registry(key='inter')
    @cli_action_version_check(min_version=(0, 0, 2))
    def interactive_cmd(self, cmd_splits: CMDSplit) -> Union["ClientCommand", Literal[False]]:
        """
        Open command in an interactive mode.
        """
        cmd_parser = cmd_parser_registry.get('inter')()
        args = cmd_parser.parse_cmd(
            cmd_splits=cmd_splits,
            strict=False
        )
        if not args:
            return False
        
        return self.send_session_cmd(
            content=cmd_parser.make_cmd_content(args),
            type='cmd'
        )
    
    @cli_registry(key='cmd')
    @cli_action_version_check(min_version=(0, 0, 2))
    def escape_cmd(self, cmd_splits: CMDSplit) -> Union["ClientCommand", Literal[False]]:
        cmd_parser = cmd_parser_registry.get('cmd')()
        args = cmd_parser.parse_cmd(
            cmd_splits=cmd_splits,
            strict=False
        )
        if not args:
            return False
        
        return self.send_session_cmd(
            content=cmd_parser.make_cmd_content(args),
            type='cmd'
        )
    
    @cli_registry(key='pexpect')
    @cli_action_version_check(min_version=(0, 0, 5))
    def pexpect_cmd(self, cmd_splits: CMDSplit) -> Union["ClientCommand", Literal[False]]:
        cmd_parser = cmd_parser_registry.get('pexpect')()
        args = cmd_parser.parse_cmd(
            cmd_splits=cmd_splits,
            strict=False
        )
        if not args:
            return False
        
        return self.send_session_cmd(
            content=cmd_parser.make_cmd_content(args),
            type='pexpect_cmd'
        )
    
    @cli_registry.register_multi([
        'ls-back',
        'ls-session',
        'ls-cmd'
    ])
    @cli_action_version_check(min_version=(0, 0, 1))
    def inner_cmd(self, cmd_splits: CMDSplit) -> Union["ClientCommand", Literal[False]]:
        possible_options, _ = cmd_splits
        if possible_options is MISSING:
            return False
        return self.send_session_cmd(
            content={
                'cmd': possible_options[0],
                'interactive': False,
                'kill_disabled': False
            },
            type='inner_cmd'
        )
    
    @cli_registry(key='server_shutdown')
    def server_shutdown(self, cmd_splits: CMDSplit) -> Literal[False]:
        try:
            confirm_key = 'YES'
            res = input(
                'WARNING: You are trying to shutdown the server. '
                'This behavior will terminate all the running '
                'commands and destroy all the sessions. Use ``ls-cmd``'
                'and ``ls-session`` to check them. If you are '
                f'sure, enter exactly "{confirm_key}", else enter '
                f'anything else. ("{confirm_key}" / [Anything else]): '
            )
            if res != confirm_key:
                logger.info(
                    'Server shutdown canceled.'
                )
                return False
            self.send_msg_to_server(type='server_shutdown')
        finally:
            return False


class Client(
    LifecycleRun,
    Thread,
    Connection,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'session_info',
        'heartbeat',
        'state',
        'cli',
        'server_writer',
        'session_writer',
        'client_listener'
    )
    
    client_registry = Registry[ActionFunc]('client_registry')
    
    def __init__(
        self,
        cli: CLI,
        address: str,
        id_prefix: Union[str, None] = None
    ) -> None:
        """
        ``id_prefix``: You can specify your own prefix to make the 
        ``session_id`` more distinguishable.
        """
        LifecycleRun.__init__(self)
        Thread.__init__(self)
        # Create a new session id.
        session_id = f'{uuid_base36(uuid.uuid1().int)}{SESSION_ID_SUFFIX}'
        if id_prefix:
            session_id = f'{id_prefix}-{session_id}'
        self.session_info = SessionInfo(address, session_id)
        self.heartbeat = Heartbeat(
            self.session_info.heartbeat_client_fp,
            self.session_info.heartbeat_session_fp
        )
        self.state = State()
        self.cli = cli
        self.server_writer = MessageHandler(self.session_info.server_listen_namespace)
        self.session_writer = MessageChannel(self.session_info.session_queue_namespace)
        self.client_listener = MessageChannel(self.session_info.client_queue_namespace)
        self.server_version: Union[Tuple[int, int, int], None] = None
        self.version_strict: bool = True
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        state = self.state
        server_check_fp = self.session_info.server_check_fp
        address = self.session_info.address
        
        # Should check whether the server exists. Otherwise, 
        # ``self.connect`` will create a new ``main_fp`` file 
        # and the real server startup will fail after that.
        if not os.path.exists(server_check_fp):
            logger.warning(
                f'Server address not found: {address}.'
            )
            state.connection_failed.set()
            return False
        
        try:
            res = self.connect()
        except BaseException as e:
            logger.error(str(e), stack_info=True)
            state.connection_failed.set()
            return False
        
        if res:
            state.connected.set()
        else:
            state.connection_failed.set()
        return res
    
    def running(self):
        address = self.session_info.address
        session_id = self.session_info.session_id
        
        logger.info(
            f'Connect with server: {address}. Session id: {session_id}. Client version: "{config.version}".'
        )
        print(CLIENT_HELP.strip('\n'))
        
        to_be_destroyed = False
        
        for _ in polling():
            try:
                if not to_be_destroyed:
                    to_be_destroyed = (not self.check_connection())
                
                msg = self.client_listener.read_one()
                if to_be_destroyed and not msg:
                    # Directly return.
                    return
                elif not msg:
                    continue
                action = dispatch_action(
                    self.client_registry,
                    msg.type,
                    f'Client'
                )
                if action is not MISSING:
                    res = action(self, msg)
                    self.check_action_result(res)
            except BaseException as e:
                logger.error(str(e), stack_info=True)
    
    def after_running(self, *args):
        self.clear_cache()
    
    #
    # Actions.
    #
    
    @client_registry(key='info')
    @param_check(required=('info',))
    def process_info(self, msg: Message) -> None:
        content = msg.content
        logger.info(f'Info from server: {content["info"]}')
    
    @client_registry(key='cmd_quit')
    @param_check(required=('cmd_id', 'type'))
    def cmd_quit(self, msg: Message) -> None:
        content = msg.content
        cmd_id = content['cmd_id']
        current_cmd = self.cli.current_cmd
        if (
            current_cmd and 
            cmd_id == current_cmd.cmd_id
        ):
            self.terminate_cmd(cause=content['type'])
    
    @client_registry(key='server_version')
    @param_check(required=('version',))
    def process_server_version(self, msg: Message) -> None:
        """
        Get server version for compatibility check.
        """
        content = msg.content
        version = content['version']
        
        try:
            self.server_version = tuple(version)
        except Exception as e:
            logger.error(str(e), stack_info=True)
    
    #
    # Connection operations.
    #
    
    def connect(self) -> bool:
        address = self.session_info.address
        session_id = self.session_info.session_id
        
        res = self.send_msg_to_server(type='new_session')
        if not res:
            return False
        if not wait_symbol(
            self.session_info.conn_client_fp
        ):
            logger.warning(
                f'Server connection establishment failed. Server address: {address}.'
                f'Session id: {session_id}.'
            )
            return False
        create_symbol(self.session_info.conn_session_fp)
        return True
    
    def disconnect(self, initiator: bool):
        logger.info(
            f'Disconnecting from server. Server: {self.session_info.address}. '
            f'Session: {self.session_info.session_id}.'
        )
        
        disconn_session_fp = self.session_info.disconn_session_fp
        disconn_confirm_to_session_fp = self.session_info.disconn_confirm_to_session_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        state = self.state
        if initiator:
            with state.terminate_lock:
                # Use lock to make it consistent.
                # Send remaining messages before disconnect.
                # Add 3 times to force kill.
                state.reset_keyboard_interrupt()
                state.add_keyboard_interrupt(3)
                # Send to server to disconnect.
                create_symbol(disconn_session_fp)
                if (
                    not wait_symbol(disconn_confirm_to_client_fp) or 
                    not wait_symbol(disconn_client_fp)
                ):
                    logger.warning(
                        'Disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                    # Set that the server is unable to communicate.
                    state.unable_to_communicate.set()
                create_symbol(disconn_confirm_to_session_fp)
                state.terminate.set()
        else:
            with state.terminate_lock:
                # Use lock to make it consistent.
                create_symbol(disconn_confirm_to_session_fp)
                # Send remaining messages.
                state.cmd_terminate_remote.set()
                create_symbol(disconn_session_fp)
                if not wait_symbol(disconn_confirm_to_client_fp):
                    logger.warning(
                        'Disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                    # Set that the server is unable to communicate.
                    state.unable_to_communicate.set()
                state.terminate.set()
        logger.info('Disconnected.')
    
    def check_connection(self) -> bool:
        disconn_client_fp = self.session_info.disconn_client_fp
        # Disconnection from remote.
        if check_symbol(disconn_client_fp):
            self.disconnect(initiator=False)
            return False
        elif (
            not self.heartbeat.beat() or 
            self.state.cli_exit.is_set()
        ):
            self.disconnect(initiator=True)
            return False
        return True
    
    #
    # Command operations.
    #
    
    def terminate_cmd(self, cause: Literal['remote', 'finish']):
        """
        Process command terminate messages.
        """
        state = self.state
        state_dict = {
            'remote': state.cmd_terminate_remote,
            'finish': state.cmd_finished
        }
        state_dict[cause].set()
    
    #
    # Other methods
    #
    
    def clear_cache(self):
        """
        Clear cached files.
        """
        if self.state.unable_to_communicate.is_set():
            # The final clear operation should be done 
            # here if the server is unable to communicate.
            self.session_info.clear_session()
    
    def check_action_result(self, res: Union[None, Tuple[str, ...]]):
        if res is None: pass
        elif isinstance(res, tuple):
            logger.warning(
                f'Missing args: {res}'
            )
    
    def send_msg_to_server(self, type: str, content: Union[dict, None] = None) -> bool:
        return self.server_writer.write(
            Message(
                session_id=self.session_info.session_id,
                type=type,
                content=content
            )
        )
    
    def send_msg_to_session(self, type: str, content: Union[dict, None] = None) -> bool:
        return self.session_writer.write(
            Message(
                session_id=self.session_info.session_id,
                type=type,
                content=content
            )
        )


class ClientCommand(
    LifecycleRun,
    Thread,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'cli',
        'msg',
        'cmd_input'
    )
    
    def __init__(
        self,
        cli: CLI,
        msg: CommandMessage,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ):
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.cli = cli
        self.msg = msg
        if self.interactive:
            self.cmd_input = InteractiveInput(self)
        else:
            self.cmd_input = NOTHING

    @property
    def cmd_id(self) -> str:
        return self.msg.cmd_id

    @property
    def interactive(self) -> bool:
        return self.msg.interactive
    
    @property
    def state(self) -> State:
        return self.cli.state

    @property
    def session_info(self) -> SessionInfo:
        return self.cli.session_info
    
    @property
    def send_msg_to_session(self):
        return self.cli.send_msg_to_session
    
    @property
    def version_strict(self) -> bool:
        return self.cli.version_strict
    
    @property
    def server_version(self) -> Union[Tuple[int, int, int], None]:
        return self.cli.server_version
    
    def before_running(self) -> bool:
        # Set that the command is running.
        self.state.set_cmd_running(True)
        return True
    
    def running(self):
        """
        Wait the session command to finish (wrapped method).
        """
        msg = self.msg
        session_info = self.session_info
        send_msg_to_session = self.send_msg_to_session
        output_namespace = session_info.message_output_namespace(msg)
        output_reader = OutputFileHandler(output_namespace)
        confirm_fp = session_info.command_terminate_confirm_fp(msg)
        state = self.state
        # The command content (for logging).
        cmd_content = msg.cmd_content
        kill_disabled = msg.kill_disabled
        # Indicate whether the current command is to be 
        # terminated (and quit the for loop if True).
        to_be_terminated = False
        # flags that indicate whether the corresponding 
        # kill_cmd messages have been sent (to avoid 
        # repeated sending).
        keyboard_interrupt_sent = False
        terminate_sent = False
        kill_sent = False
        
        if kill_disabled:
            logger.info(
                'You are disabling the Keyboard Interrupt to kill a command, '
                'and you need to manually quit the command using specified input '
                '(e.g., ``exit()`` in Python, ``exit`` in bash, etc.).'
            )
        
        #
        # For loop functions (to make the for loop clear and more 
        # readable).
        #
        def _check_cmd_state() -> bool:
            """
            Check command state. Return False if the command is ready 
            to quit.
            """
            if state.cmd_finished.is_set():
                # Clear the output before terminate.
                self.redirect_output(output_reader, read_all=True)
                return False
            if (
                state.cmd_terminate_remote.is_set() or 
                state.terminate.is_set() or 
                to_be_terminated
            ):
                # ``to_be_terminated``: the client has already request the session 
                # to terminate the command, no matter what the response is, or the 
                # session has confirmed that the command has been terminated.
                # NOTE: If keyboard_interrupt >= 4, the process will be put to the 
                # background. However, the process may still endlessly produce output, 
                # causing the cli unable to normally quit, so all the remaining content 
                # should be read within a set timeout, and directly break.
                self.redirect_output(
                    output_reader,
                    read_all=True,
                    read_all_timeout=config.cmd_client_read_timeout
                )
                return False
            return True
        
        def _send_kill_cmd_msg(type: str):
            """
            Send the kill_cmd msg with given type.
            """
            if (
                self.version_strict and 
                not version_check(
                    self.server_version,
                    min_version=(0, 0, 2)
                )
            ):
                return
            send_msg_to_session(
                type='kill_cmd',
                content={
                    'cmd_id': msg.cmd_id,
                    'type': type
                }
            )
        
        def _process_keyboard_interrupt():
            """
            Process the keyboard interrupt.
            """
            nonlocal to_be_terminated
            nonlocal kill_sent, terminate_sent, keyboard_interrupt_sent
            keyboard_interrupt_cnt = state.get_keyboard_interrupt()
            
            if kill_disabled:
                # All the keyboard interrupts are treated as plain 
                # signals rather than killing requests.
                if keyboard_interrupt_cnt > 0:
                    # Send keyboard interrupt.
                    _send_kill_cmd_msg(type='keyboard')
                    # Reset the keyboard interrupt to query more 
                    # user ``Ctrl+C`` press.
                    state.reset_keyboard_interrupt()
            elif keyboard_interrupt_cnt >= CMD_BACKGROUND:
                # Put the command to the background.
                send_msg_to_session(
                    type='background_cmd',
                    content={'cmd_id': msg.cmd_id}
                )
                logger.info(
                    'Command is now put to the background, and use '
                    '``ls-back`` to check its state.'
                )
                # Set ``to_be_terminated``
                to_be_terminated = True
            elif (
                keyboard_interrupt_cnt == CMD_KILL and 
                not kill_sent
            ):
                # Kill the command (send SIGKILL)
                logger.info(f'Killing session command: {cmd_content}.')
                _send_kill_cmd_msg(type='force')
                kill_sent = True
            elif (
                keyboard_interrupt_cnt == CMD_TERMINATE and 
                not terminate_sent
            ):
                # Terminate the command (send SIGTERM)
                logger.info(f'Terminating session command: {cmd_content}.')
                _send_kill_cmd_msg(type='remote')
                terminate_sent = True
            elif (
                keyboard_interrupt_cnt == CMD_INTERRUPT and 
                not keyboard_interrupt_sent
            ):
                # Send keyboard interrupt (send SIGINT)
                logger.info(
                    f'Sending keyboard interrupt to session command: {cmd_content}.'
                )
                _send_kill_cmd_msg(type='keyboard')
                keyboard_interrupt_sent = True
        
        for _ in polling(config.cmd_polling_interval):
            # Check the command state.
            if not _check_cmd_state():
                break
            # Print the output content to the fake_cmd.
            self.redirect_output(output_reader, read_all=False)
            # Process keyboard interrupt.
            _process_keyboard_interrupt()
            if check_symbol(confirm_fp):
                logger.info(f'Command successfully terminated.')
                # Set ``to_be_terminated``
                to_be_terminated = True
    
    def after_running(self, __exc_type=None, __exc_value=None, __traceback=None):
        session_info = self.session_info
        msg = self.msg
        confirm_fp = session_info.command_terminate_confirm_fp(msg)
        output_namespace = session_info.message_output_namespace(msg)
        
        if (
            __exc_type is not None or 
            __exc_value is not None or 
            __traceback is not None
        ):
            logger.error(
                f'Exception occurred in command. {str(__exc_type)} - {str(__exc_value)}',
                stack_info=True
            )
        # Remove all files.
        remove_file_with_retry(confirm_fp)
        remove_dir_with_retry(output_namespace)
        state = self.state
        with state.cmd_running_lock:
            if self.interactive:
                if not self.cmd_input.quit.is_set():
                    logger.info(
                        'The command has exited, and you need to press '
                        '``Ctrl+C`` or ``Enter`` to further quit the '
                        'interactive mode.'
                    )
            # Set that the command is not running.
            state.set_cmd_running(False)
    
    def redirect_output(
        self,
        reader: OutputFileHandler,
        read_all: bool,
        read_all_timeout: Union[float, Missing] = MISSING
    ) -> bool:
        """
        Redirect the content of ``fp`` to the cli. Return whether 
        any content exists.
        
        ``read_all_timeout``: only works when ``read_all`` is True.
        """
        content = ''
        if read_all:
            content = reader.read_all(timeout=read_all_timeout)
        else:
            content = reader.read_one()
        # NOTE: If ``read_all`` is ``True``, then we can 
        # directly judge there is no content to read and 
        # return ``False``. However, if ``read_all`` is 
        # ``False``, we just read one file from the 
        # namespace, and if ``content`` is an empty str, 
        # it does not represent that no more content is 
        # available, so we should judge whether ``content``
        # is exactly ``False`` rather than using 
        # ``not content``.
        if (
            (read_all and (not content)) or 
            ((not read_all) and content is False)
        ):
            return False
        sys.stdout.write(content)
        sys.stdout.flush()
        return True
    
    def safely_check_cmd_running(self) -> bool:
        """
        Safely check the command running states.
        """
        state = self.state
        running = True
        if not state.cmd_running.is_set():
            running = False
        with state.cmd_running_lock:
            if not state.cmd_running.is_set():
                running = False
        return (running or self.is_alive())
    
    def __bool__(self) -> bool:
        return True


class InteractiveInput(
    LifecycleRun,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    Accept user input and send it to the remote session command.
    
    NOTE: fake_cmd simply displays the output from the session command, 
    and it doesn't exactly know what the input prompt is (in the session 
    command), so ``InteractiveInput`` may cause messed-up display (
    especially when using ``readline``). Pressing EOF (e.g., ``Ctrl-D`` 
    in Unix, ``Ctrl-Z`` in Windows, etc.) to start a new line can alleviate 
    this problem.
    """
    readonly_attr__ = (
        'cmd',
        'quit'
    )
    
    def __init__(self, cmd: ClientCommand) -> None:
        LifecycleRun.__init__(self)
        ReadonlyAttr.__init__(self)
        self.cmd = cmd
        self.quit = Event()
    
    @property
    def msg(self) -> CommandMessage:
        return self.cmd.msg
    
    @property
    def cmd_id(self) -> str:
        return self.msg.cmd_id
    
    @property
    def send_msg_to_session(self):
        return self.cmd.send_msg_to_session
    
    @property
    def session_id(self) -> str:
        return self.cmd.session_info.session_id
    
    @property
    def state(self) -> State:
        return self.cmd.state
    
    @property
    def cmd_running(self) -> bool:
        return self.cmd.safely_check_cmd_running()
    
    def before_running(self) -> bool:
        self.quit.clear()
        return True
    
    def running(self) -> None:
        logger.info(
            f'You are in the interactive mode.'
        )
        send_msg_to_session = self.send_msg_to_session
        state = self.state
        while True:
            try:
                # NOTE: fake_cmd doesn't know what the input prompt 
                # is, so the prompt is empty here, and the remote 
                # output will be simultaneously displayed in the CLI. 
                # This may cause messed-up display (especially when 
                # using modules like ``readline``).
                input_str = input()
                if not self.cmd_running:
                    raise EOFError
                send_msg_to_session(
                    type='cmd_input',
                    content={
                        'cmd_id': self.cmd_id,
                        'input_str': input_str
                    }
                )
            except KeyboardInterrupt:
                with ignore_keyboard_interrupt():
                    if not self.cmd_running:
                        logger.info(f'Interactive mode quitted.')
                        return
                    print('Keyboard Interrupt (Interactive Input).')
                    state.add_keyboard_interrupt()
            except EOFError:
                # NOTE: ``EOFError`` can be used to start a new line to 
                # avoid messed-up display caused by simultaneous remote 
                # output and client input.
                with ignore_keyboard_interrupt():
                    if not self.cmd_running:
                        logger.info(f'Interactive mode quitted.')
                        return
                    print('EOF (Interactive Input).')
            except Exception as e:
                logger.error(str(e), stack_info=True)
    
    def after_running(self, __exc_type=None, __exc_value=None, __traceback=None):
        self.quit.set()
