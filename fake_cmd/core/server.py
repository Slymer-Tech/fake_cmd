import os
import time
from threading import Thread, Event, RLock
from abc import ABCMeta
from slime_core.utils.common import FuncParams
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import (
    Metaclasses,
    _ReadonlyAttrMetaclass,
    InitOnceMetaclass
)
from slime_core.utils.base import BaseList
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING,
    Iterable,
    Missing,
    Literal,
    Callable,
    Any,
    Nothing,
    Tuple
)
from fake_cmd.utils import (
    config
)
from fake_cmd.utils.comm import (
    Connection,
    Message,
    create_symbol,
    wait_symbol,
    check_symbol,
    Heartbeat,
    CommandMessage,
    MessageHandler,
    MessageChannel,
    OutputFileHandler
)
from fake_cmd.utils.common import polling, timestamp_to_str
from fake_cmd.utils.parallel import (
    CommandPool,
    LifecycleRun,
    ExitCallbackFunc
)
from fake_cmd.utils.executors import (
    Executor,
    platform_open_executor_registry,
    DefaultPopenExecutor,
    pexpect_executor_registry,
    PexpectExecutor
)
from fake_cmd.utils.executors.reader import (
    PopenReader,
    popen_reader_registry,
    PipePopenReader,
    pexpect_reader_registry,
    DefaultPexpectReader,
    PexpectReader
)
from fake_cmd.utils.executors.writer import PopenWriter, popen_writer_registry
from fake_cmd.utils.exception import ServerShutdown, ignore_keyboard_interrupt
from fake_cmd.utils.logging import logger
from . import ServerInfo, SessionInfo, dispatch_action, ActionFunc, param_check

#
# States
#

class SessionState(ReadonlyAttr):
    
    readonly_attr__ = (
        'destroy_local',
        'unable_to_communicate'
    )
    
    def __init__(self) -> None:
        self.destroy_local = Event()
        # Unable to communicate to client, so in 
        # ``clear_cache``, it will clear the namespace 
        # ignoring whether the client has already 
        # finished reading messages.
        self.unable_to_communicate = Event()


class CommandState(ReadonlyAttr):
    
    readonly_attr__ = (
        'keyboard_interrupt_remote',
        'kill_disabled',
        'terminate_local',
        'terminate_remote',
        'terminate_disconnect',
        'force_kill',
        'finished',
        'exit',
        'queued',
        'scheduled',
        'scheduled_lock'
    )
    
    def __init__(self) -> None:
        # Keyboard interrupt from remote.
        self.keyboard_interrupt = Event()
        # Whether keyboard interrupt is not used to kill the command.
        self.kill_disabled = Event()
        # Terminate from local.
        self.terminate_local = Event()
        # Terminate from remote.
        self.terminate_remote = Event()
        # Terminate because of disconnection.
        self.terminate_disconnect = Event()
        # Force kill.
        self.force_kill = Event()
        # Mark that the command normally finished.
        self.finished = Event()
        # Mark that the command finally exit, whether through itself 
        # or by manual termination (usually because the command is 
        # terminated before being scheduled).
        self.exit = Event()
        # Command is queued. Used only to decide whether to 
        # notify the client that the task is queued.
        self.queued = Event()
        # The command is scheduled. Compared to ``queued``, it 
        # is more safe because of using a ``RLock``.
        self.scheduled = Event()
        self.scheduled_lock = RLock()
    
    @property
    def pending_terminate(self) -> bool:
        return (
            self.terminate_local.is_set() or 
            self.terminate_remote.is_set() or 
            self.terminate_disconnect.is_set() or 
            self.force_kill.is_set() or 
            (
                self.keyboard_interrupt.is_set() and
                not self.kill_disabled.is_set()
            )
        )

#
# Server
#

class Server(
    LifecycleRun,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'server_info',
        'session_dict',
        'cmd_pool'
    )
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('server_action')
    
    def __init__(
        self,
        address: str,
        max_cmds: Union[int, None] = None
    ) -> None:
        LifecycleRun.__init__(self)
        self.server_info = ServerInfo(address)
        self.session_dict: Dict[str, Session] = {}
        self.cmd_pool = CommandPool(max_cmds)
        self.server_listener: Union[MessageHandler, Nothing] = NOTHING
    
    #
    # Running operations.
    #

    def before_running(self) -> bool:
        listen_fp = self.server_info.server_check_fp
        address = self.server_info.address
        if os.path.exists(listen_fp):
            logger.warning(
                f'server.listen file already exists at {address}, may be another '
                'server is running at the same address. Check the address setting '
                'and if you are sure no other servers are running at the address, '
                f'you may need to manually remove the file: "{listen_fp}".'
            )
            return False
        
        # file initialization
        self.server_info.init_server()
        # Init server listener
        self.server_listener = MessageHandler(self.server_info.server_listen_namespace)
        # NOTE: Open cmd_pool here.
        self.cmd_pool.start()
        logger.info(f'Server initialized. Address {address} created.')
        return True

    def running(self):
        address = self.server_info.address
        
        logger.info(f'Server started. Listening at: {address}')
        for msg in self.server_listener.listen():
            try:
                action = dispatch_action(
                    self.action_registry,
                    msg.type,
                    'Main Server'
                )
                if action is not MISSING:
                    res = action(self, msg)
                    self.check_action_result(res)
            except ServerShutdown:
                return
    
    def after_running(self, *args):
        with ignore_keyboard_interrupt():
            logger.info('Server shutting down...')
            self.cmd_pool.pool_close.set()
            # Destroy sessions.
            # Use a new tuple, because the ``exit_callback`` will pop 
            # the dict items.
            sessions = tuple(self.session_dict.values())
            for session in sessions:
                session.session_state.destroy_local.set()
            logger.info('Waiting sessions to destroy...')
            for session in sessions:
                session.join(config.server_shutdown_wait_timeout)
            # Kill all the commands in the command pool.
            for cmd in tuple(self.cmd_pool.update_and_get_execute()):
                cmd.executor.kill()
            # Check the shutdown state.
            if any(map(lambda session: session.is_alive(), sessions)):
                logger.warning(
                    'Warning: there are still sessions undestroyed. Ignoring and shutdown...'
                )
            else:
                logger.info('Successfully shutdown. Bye.')
            self.server_info.clear_server()
    
    @action_registry(key='new_session')
    def create_new_session(self, msg: Message):
        session_id = msg.session_id
        
        def destroy_session_func(*args):
            """
            Passed to the session to call when destroy.
            """
            self.pop_session_dict(session_id)
        
        session = Session(
            self,
            SessionInfo(self.server_info.address, session_id),
            exit_callbacks=[destroy_session_func]
        )
        if session_id in self.session_dict:
            logger.warning(
                f'Session_id {session_id} already exists and the creation will be ignored.'
            )
            return
        
        self.session_dict[session_id] = session
        session.start()
    
    @action_registry(key='destroy_session')
    @param_check(required=('session_id',))
    def destroy_session(self, msg: Message):
        session_id = msg.content['session_id']
        if session_id not in self.session_dict:
            logger.warning(
                f'Session id {session_id} not in the session dict.'
            )
            return
        session = self.session_dict[session_id]
        state = session.session_state
        state.destroy_local.set()
    
    @action_registry(key='server_shutdown')
    def server_shutdown(self, msg: Message):
        logger.info(
            f'Server shutdown received from: "{msg.session_id}".'
        )
        raise ServerShutdown
    
    def pop_session_dict(self, session_id: str):
        return self.session_dict.pop(session_id, None)
    
    def check_action_result(self, res: Union[None, Tuple[str, ...]]):
        if res is None: pass
        elif isinstance(res, tuple):
            logger.warning(
                f'Missing args: {res}'
            )

#
# Session
#

class Session(
    LifecycleRun,
    Thread,
    Connection,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    One session can only run one command at the same time.
    """
    readonly_attr__ = (
        'server',
        'session_info',
        'session_state',
        'heartbeat',
        'running_cmd_lock',
        'background_cmds',
        'created_timestamp',
        'session_listener',
        'client_writer'
    )
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('session_action')
    
    def __init__(
        self,
        server: Server,
        session_info: SessionInfo,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.server = server
        self.session_info = session_info
        self.session_state = SessionState()
        self.heartbeat = Heartbeat(
            self.session_info.heartbeat_session_fp,
            self.session_info.heartbeat_client_fp
        )
        self.running_cmd: Union[SessionCommand, None] = None
        self.running_cmd_lock = RLock()
        self.background_cmds = BackgroundCommandList()
        # Created time.
        self.created_timestamp = time.time()
        # file initialization
        self.session_info.init_session()
        # Message communication
        self.session_listener = MessageChannel(self.session_info.session_queue_namespace)
        self.client_writer = MessageChannel(self.session_info.client_queue_namespace)
        logger.info(f'Session {self.session_id} created.')
    
    @property
    def cmd_pool(self) -> CommandPool:
        return self.server.cmd_pool
    
    @property
    def session_id(self) -> str:
        return self.session_info.session_id
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return self.connect()
    
    def running(self):
        # Send the server version.
        self.send_msg_to_client(
            type='server_version',
            content={
                'version': list(config.version)
            }
        )
        
        # Start the session listening.
        to_be_destroyed = False
        session_id = self.session_id
        action_registry = self.action_registry
        
        for _ in polling():
            if not to_be_destroyed:
                to_be_destroyed = (not self.check_connection())
            
            msg = self.session_listener.read_one()
            if to_be_destroyed and not msg:
                return
            elif not msg:
                continue
            action = dispatch_action(
                action_registry,
                msg.type,
                f'Server Session {session_id}'
            )
            if action is not MISSING:
                res = action(self, msg)
                self.check_action_result(res, msg)
    
    def after_running(self, *args):
        # Further confirm the command is terminated.
        self.safely_terminate_cmd(cause='destroy')
        self.clear_cache()
        # Kill all the background commands.
        for back_cmd in tuple(self.background_cmds.update_and_get()):
            back_cmd.executor.kill()
        
        # Wait all the commands to finish.
        running_cmd = self.running_cmd
        if running_cmd:
            running_cmd.join()
        for back_cmd in tuple(self.background_cmds.update_and_get()):
            back_cmd.join()
    
    #
    # Actions.
    #
    
    @action_registry(key='cmd')
    @param_check(required=(
        'cmd', 'encoding', 'reader', 'writer', 'exec', 
        'platform', 'kill_disabled', 'interactive'
    ))
    def run_new_cmd(self, msg: Message):
        """
        Run a new Popen command.
        """
        def terminate_command_func(*args):
            self.cmd_pool.cancel(cmd)
            self.reset_running_cmd()
        
        cmd = PopenShellCommand(
            session=self,
            msg=msg,
            exit_callbacks=[terminate_command_func]
        )
        self.set_running_cmd(cmd)
        self.cmd_pool.submit(cmd)
    
    @action_registry(key='pexpect_cmd')
    @param_check(required=[
        'cmd', 'encoding', 'reader', 'kill_disabled', 'interactive',
        'echo'
    ])
    def run_pexpect_cmd(self, msg: Message):
        """
        Run a new pexpect command.
        """
        def terminate_command_func(*args):
            self.cmd_pool.cancel(cmd)
            self.reset_running_cmd()
        
        cmd = PexpectShellCommand(
            session=self,
            msg=msg,
            exit_callbacks=[terminate_command_func]
        )
        self.set_running_cmd(cmd)
        self.cmd_pool.submit(cmd)
    
    @action_registry(key='inner_cmd')
    @param_check(required=('cmd',))
    def run_inner_cmd(self, msg: Message):
        def terminate_command_func(*args):
            self.reset_running_cmd()
        
        cmd = InnerCommand(
            session=self,
            msg=msg,
            exit_callbacks=[terminate_command_func]
        )
        self.set_running_cmd(cmd)
        cmd.start()
    
    @action_registry(key='kill_cmd')
    @param_check(required=('cmd_id', 'type'))
    def kill_cmd(self, msg: Message):
        cmd_id = msg.content['cmd_id']
        type = msg.content['type']
        with self.running_cmd_lock:
            running_cmd = self.running_cmd_check(cmd_id)
            if not running_cmd:
                return
            self.safely_terminate_cmd(cause=type)
    
    @action_registry(key='background_cmd')
    @param_check(required=('cmd_id',))
    def make_cmd_background(self, msg: Message):
        cmd_id = msg.content['cmd_id']
        with self.running_cmd_lock:
            running_cmd = self.running_cmd_check(cmd_id)
            if not running_cmd:
                return
            self.background_cmds.append(running_cmd)
            self.reset_running_cmd()
    
    @action_registry(key='cmd_input')
    @param_check(required=('cmd_id', 'input_str'))
    def input_to_cmd(self, msg: Message):
        cmd_id = msg.content['cmd_id']
        input_str = msg.content['input_str']
        
        with self.running_cmd_lock:
            running_cmd = self.running_cmd_check(cmd_id)
            if not running_cmd:
                return
            
            executor = running_cmd.executor
            if not executor.writable():
                self.info_client(f'Input failed: Current cmd {str(running_cmd)} does not accept input.')
                return
            if not executor.is_started():
                self.info_client(f'Input failed: Command {str(running_cmd)} has not started yet.')
                return
            if not executor.is_running():
                self.info_client(f'Input failed: Command {str(running_cmd)} is not running.')
                return
            
            # Write to the cmd.
            res = executor.write_line(input_str)
            if not res:
                self.info_client(f'Interactive input failed: "{input_str}".')
    
    def running_cmd_check(self, cmd_id: str) -> Union["SessionCommand", Literal[False]]:
        """
        Check if the msg command id is equal to that of 
        the running command.
        """
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if not running_cmd:
                return False
            
            if running_cmd.cmd_id != cmd_id:
                self.notify_cmd_inconsistency(running_cmd, cmd_id)
                return False
            
            return running_cmd
    
    def notify_cmd_inconsistency(
        self,
        running_cmd: "SessionCommand",
        incoming_cmd_id: str
    ):
        """
        Notify the command id inconsistency.
        """
        self.info_client(
            f'Command running inconsistency occurred. '
            f'Requiring from client: {incoming_cmd_id}. Actual running: '
            f'{running_cmd.cmd_id}'
        )
    
    #
    # Command operations.
    #
    
    def safely_terminate_cmd(
        self,
        cmd: Union["SessionCommand", Missing] = MISSING,
        cause: Literal[
            'remote', 'local', 'destroy', 'disconnect', 'force', 'keyboard'
        ] = 'remote'
    ):
        """
        Safely terminate the cmd whether it is running, queued or finished. 
        ``destroy``: called before the session destroyed to further make sure 
        the command is terminated.
        """
        with self.running_cmd_lock:
            if cmd is MISSING:
                cmd = self.running_cmd
            
            if self.running_cmd is not cmd:
                logger.warning(
                    f'Running cmd inconsistency occurred.'
                )
            if not cmd:
                return
            
            cmd_state = cmd.cmd_state
            with cmd_state.scheduled_lock:
                cause_dict = {
                    'remote': cmd_state.terminate_remote,
                    'local': cmd_state.terminate_local,
                    'disconnect': cmd_state.terminate_disconnect,
                    'force': cmd_state.force_kill,
                    'keyboard': cmd_state.keyboard_interrupt
                }
                if cause in cause_dict:
                    cause_dict[cause].set()

                # If currently ``scheduled`` is not set, then it will never 
                # be scheduled by the pool (because terminate state is set 
                # under the ``scheduled_lock``).
                scheduled = cmd_state.scheduled.is_set()
            
            if not scheduled:
                # Manually call the exit callbacks and ``after_running``, 
                # because it will never be scheduled.
                cmd.run_exit_callbacks__()
                cmd.after_running()
    
    def set_running_cmd(self, running_cmd: Union["SessionCommand", None]):
        """
        Return whether the set operation succeeded.
        """
        with self.running_cmd_lock:
            if (
                self.running_cmd and 
                running_cmd
            ):
                # One command is running or has not been terminated, but 
                # another command received.
                self.info_client(
                    'Another command is running or has not been '
                    'terminated, inconsistency occurred. Use ``ls-cmd`` '
                    'to check it.'
                )
            self.running_cmd = running_cmd
    
    def reset_running_cmd(self):
        self.set_running_cmd(None)
    
    #
    # Connection operations.
    #
    
    def connect(self) -> bool:
        create_symbol(self.session_info.conn_client_fp)
        if not wait_symbol(
            self.session_info.conn_session_fp
        ):
            logger.warning(
                f'Connection establishment failed. Missing client response. '
                f'Server address: {self.session_info.address}. Session id: {self.session_id}'
            )
            return False
        return True
    
    def disconnect(self, initiator: bool):
        disconn_session_fp = self.session_info.disconn_session_fp
        disconn_confirm_to_session_fp = self.session_info.disconn_confirm_to_session_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        
        if initiator:
            self.safely_terminate_cmd(cause='disconnect')
            create_symbol(disconn_client_fp)
            if (
                not wait_symbol(disconn_confirm_to_session_fp) or 
                not wait_symbol(disconn_session_fp)
            ):
                logger.warning(
                    'Disconnection from client is not responded, '
                    'ignore and continue...'
                )
                # Set the client is unable to communicate.
                self.session_state.unable_to_communicate.set()
            create_symbol(disconn_confirm_to_client_fp)
        else:
            create_symbol(disconn_confirm_to_client_fp)
            self.safely_terminate_cmd(cause='remote')
            create_symbol(disconn_client_fp)
            if not wait_symbol(disconn_confirm_to_session_fp):
                logger.warning(
                    'Disconnection from client is not responded, '
                    'ignore and continue...'
                )
                # Set the client is unable to communicate.
                self.session_state.unable_to_communicate.set()
        logger.info(
            f'Successfully disconnect session: {self.session_id}'
        )
    
    def check_connection(self) -> bool:
        disconn_session_fp = self.session_info.disconn_session_fp
        session_state = self.session_state
        
        with self.running_cmd_lock:
            if check_symbol(disconn_session_fp):
                self.disconnect(initiator=False)
                return False
            elif (
                session_state.destroy_local.is_set() or 
                not self.heartbeat.beat()
            ):
                self.disconnect(initiator=True)
                return False
        return True
    
    #
    # Other methods.
    #
    
    def clear_cache(self):
        """
        Clear the cached files.
        """
        if self.session_state.unable_to_communicate.is_set():
            self.session_info.clear_session()
            return
        # Wait whether the client has received the final ``disconn_confirm_to_client_fp``
        # when ``self.disconnect(initiator=True)``
        wait_symbol(self.session_info.disconn_confirm_to_client_fp, wait_for_remove=True)
        # Clear anyway.
        self.session_info.clear_session()
    
    def __str__(self) -> str:
        return (
            f'Session(session_id="{self.session_id}", '
            f'created_time={timestamp_to_str(self.created_timestamp)})'
        )
    
    def check_action_result(self, res: Union[None, Tuple[str, ...]], msg: Message):
        if res is None: pass
        elif isinstance(res, tuple):
            logger.warning(
                f'Missing args: {res}'
            )
            self.info_client(f'Server action missing args: {res}')
            if msg.type in ('cmd', 'inner_cmd', 'pexpect_cmd'):
                # Notify that the command has terminated.
                self.send_msg_to_client(
                    type='cmd_quit',
                    content={
                        'cmd_id': msg.msg_id,
                        'type': 'remote'
                    }
                )
    
    #
    # Message sending wrappers.
    #
    
    def send_msg_to_client(self, type: str, content: Union[dict, None] = None) -> bool:
        """
        Send message to the corresponding client.
        """
        self.client_writer.write(
            Message(
                session_id=self.session_id,
                type=type,
                content=content
            )
        )
    
    def info_client(self, info: str) -> bool:
        """
        Send ``info`` message to the client.
        """
        return self.send_msg_to_client(type='info', content={'info': info})

#
# Commands
#

# TODO: We are considering changing the ``SessionCommand`` from a ``Thread`` to a 
# ``Process``, because ``os.fork`` and ``os.forkpty`` may cause deadlock in the 
# multithreading mode (and the usage is deprecated since Python 3.12). This problem 
# may involve the ``pexpect`` module.
class SessionCommand(
    LifecycleRun,
    Thread,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    Start running a new command in the session.
    """
    readonly_attr__ = (
        'session',
        'msg',
        'executor',
        'cmd_state',
        'output_writer'
    )
    
    def __init__(
        self,
        session: Session,
        msg: Message,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None,
        executor: Union[Executor, Nothing] = NOTHING
    ):
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.session = session
        self.msg = CommandMessage.clone(msg)
        # The running process executor (if any).
        self.executor = executor
        self.cmd_state = CommandState()
        self.output_writer = OutputFileHandler(self.output_namespace)
    
    @property
    def session_info(self) -> SessionInfo:
        return self.session.session_info
    
    @property
    def cmd_id(self) -> str:
        return self.msg.cmd_id
    
    @property
    def output_namespace(self) -> str:
        return self.session_info.message_output_namespace(self.msg)
    
    @property
    def client_writer(self) -> MessageHandler:
        return self.session.client_writer
    
    @property
    def send_msg_to_client(self):
        return self.session.send_msg_to_client
    
    @property
    def info_client(self):
        return self.session.info_client
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        self.info_running()
        self.cmd_state.queued.clear()
        return True
    
    def after_running(self, __exc_type=None, __exc_value=None, __traceback=None):
        cmd_state = self.cmd_state
        session_info = self.session_info
        # Priority: Remote > Local > Finished
        # Because if terminate remote is set, the client is waiting a 
        # terminate confirm, so its priority should be the first.
        if (
            cmd_state.terminate_remote.is_set() or 
            cmd_state.force_kill.is_set() or 
            cmd_state.keyboard_interrupt.is_set()
        ):
            create_symbol(
                session_info.command_terminate_confirm_fp(self.msg)
            )
        elif cmd_state.terminate_local.is_set():
            self.send_msg_to_client(
                type='cmd_quit',
                content={
                    'cmd_id': self.cmd_id,
                    'type': 'remote'
                }
            )
        elif cmd_state.finished.is_set():
            self.send_msg_to_client(
                type='cmd_quit',
                content={
                    'cmd_id': self.cmd_id,
                    'type': 'finish'
                }
            )
        elif cmd_state.terminate_disconnect.is_set():
            # Doing nothing because the disconnect operations 
            # will automatically notify the client.
            pass
        
        if (
            __exc_type or 
            __exc_value or 
            __traceback
        ):
            self.info_client(
                f'Command terminated with exception: {str(__exc_type)} - '
                f'{str(__exc_value)}. Command content: {self.msg.content}. '
                f'Command id: {self.cmd_id}.'
            )
            self.send_msg_to_client(
                type='cmd_quit',
                content={
                    'cmd_id': self.cmd_id,
                    'type': 'remote'
                }
            )
        # Mark the command exits.
        cmd_state.exit.set()
    
    #
    # Client info.
    #
    
    def info_running(self):
        """
        Notify the client that the command is running.
        """
        if self.cmd_state.queued.is_set():
            # Notify only when it is queued.
            self.info_client(f'Command "{self.msg.cmd_id}" running...')
    
    def info_queued(self):
        """
        Notify the client that the command is queued.
        """
        if self.cmd_state.queued.is_set():
            self.info_client(f'Command "{self.msg.cmd_id}" being queued...')
    
    #
    # Other methods.
    #
    
    def __str__(self) -> str:
        return (
            f'Command(cmd="{self.msg.content["cmd"]}", cmd_id="{self.msg.cmd_id}", '
            f'session_id="{self.session_info.session_id}", '
            f'created_time={timestamp_to_str(self.msg.timestamp)}, '
            f'executor={str(self.executor)})'
        )
    
    def __bool__(self) -> bool:
        return True


class ShellCommand(SessionCommand):
    """
    Start a new shell command.
    """
    
    def __init__(
        self,
        session: Session,
        msg: Message,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None,
        executor: Union[Executor, Nothing] = NOTHING
    ):
        SessionCommand.__init__(
            self,
            session=session,
            msg=msg,
            exit_callbacks=exit_callbacks,
            executor=executor
        )
        # NOTE: This should be called after the Message object has been converted to 
        # a CommandMessage object in SessionCommand.
        if self.msg.kill_disabled:
            # Set kill disabled.
            self.cmd_state.kill_disabled.set()
    
    def running(self) -> None:
        # Start the command exec.
        executor = self.executor
        state = self.cmd_state
        # Flags indicating wether the signals have been 
        # sent.
        interrupt_sent = False
        terminate_sent = False
        kill_sent = False
        reader_closed = False
        writer_closed = False
        
        def _process_kill_signals():
            """
            Process the kill signals (SIGINT, SIGTERM, SIGKILL, etc.)
            """
            nonlocal kill_sent, terminate_sent, interrupt_sent
            nonlocal reader_closed, writer_closed
            if (
                (
                    state.terminate_local.is_set() or 
                    state.terminate_disconnect.is_set() or 
                    state.force_kill.is_set()
                ) and not kill_sent
            ):
                # Directly kill the process.
                executor.kill()
                kill_sent = True
            elif (
                state.terminate_remote.is_set() and 
                not terminate_sent
            ):
                # Terminate the process.
                executor.terminate()
                terminate_sent = True
            elif state.kill_disabled.is_set():
                if state.keyboard_interrupt.is_set():
                    # Can send keyboard interrupt multiple times 
                    # according to the user behavior.
                    executor.keyboard_interrupt()
                    state.keyboard_interrupt.clear()
            elif (
                state.keyboard_interrupt.is_set() and 
                not interrupt_sent
            ):
                # Send keyboard interrupt.
                executor.keyboard_interrupt()
                interrupt_sent = True
            
            if state.pending_terminate:
                # NOTE: Should close read and write here if possible. 
                # Otherwise, writers like 'pty' will never be closed. 
                # ``PipePopenWriter`` will do nothing here, because 
                # the ``__exit__`` of ``executor`` will close stdin, 
                # stdout and stderr automatically.
                if not reader_closed:
                    executor.close_read()
                    reader_closed = True
                if not writer_closed:
                    executor.close_write()
                    writer_closed = True
        
        with executor:
            for _ in polling(config.cmd_polling_interval):
                # NOTE: DO NOT return after the signal is sent, 
                # and it should always wait until the process 
                # quits.
                _process_kill_signals()
                # Write outputs.
                self.output_writer.write(
                    executor.read(config.cmd_pipe_read_timeout)
                )
                
                if not executor.is_running():
                    break
            
            if not state.pending_terminate:
                # Read all the remaining content.
                self.output_writer.write(executor.read_all())
                # Normally finished.
                state.finished.set()
            else:
                # Read the remaining output within a timeout. This is because 
                # if the command is not properly terminated (e.g., the main Popen 
                # process is killed, but its subprocesses are still alive and 
                # producing outputs), the command may get stuck reading endlessly.
                self.output_writer.write(
                    executor.read(config.cmd_pipe_read_timeout_when_terminate)
                )


class PopenShellCommand(ShellCommand):
    """
    Start a new shell command using Popen.
    """
    
    def __init__(
        self,
        session: Session,
        msg: Message,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ):
        content: dict = msg.content
        encoding: str = content['encoding'] or config.cmd_pipe_encoding
        reader: PopenReader = popen_reader_registry.get(content['reader'], PipePopenReader)()
        writer: PopenWriter = popen_writer_registry.get(content['writer'], PopenWriter)()
        exec: Union[str, None] = content['exec'] or config.cmd_executable
        platform: str = content['platform'] or config.platform
        # Create Executor object.
        executor = platform_open_executor_registry.get(platform, DefaultPopenExecutor)(
            popen_params=FuncParams(
                content['cmd'],
                shell=True,
                stdin=writer.get_popen_stdin(),
                stdout=reader.get_popen_stdout(),
                stderr=reader.get_popen_stderr(),
                executable=exec,
                bufsize=0,
                text=False
            ),
            reader=reader,
            writer=writer,
            encoding=encoding
        )
        ShellCommand.__init__(
            self,
            session=session,
            msg=msg,
            exit_callbacks=exit_callbacks,
            executor=executor
        )


class PexpectShellCommand(ShellCommand):
    """
    Start a new shell command using pexpect.
    """
    
    def __init__(
        self,
        session: Session,
        msg: Message,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ):
        content: dict = msg.content
        reader: PexpectReader = pexpect_reader_registry.get(content['reader'], DefaultPexpectReader)()
        encoding: str = content['encoding'] or config.cmd_pipe_encoding
        executor = pexpect_executor_registry.get('default', PexpectExecutor)(
            pexpect_params=FuncParams(
                content['cmd']
            ),
            reader=reader,
            encoding=encoding,
            echo=content['echo']
        )
        ShellCommand.__init__(
            self,
            session=session,
            msg=msg,
            exit_callbacks=exit_callbacks,
            executor=executor
        )


class InnerCommand(SessionCommand):
    """
    Start an Inner Command.
    """
    
    inner_cmd_registry = Registry[Callable[[Any], None]]('inner_cmd_registry')
    
    def running(self):
        msg = self.msg
        content = msg.content
        cmd = content['cmd']
        action = dispatch_action(
            self.inner_cmd_registry,
            cmd,
            f'Session inner command: {cmd}. Session id: {self.session_info.session_id}'
        )
        if action is not MISSING:
            action(self)
        
        state = self.cmd_state
        if not state.pending_terminate:
            state.finished.set()
    
    @inner_cmd_registry(key='ls-back')
    def list_background(self):
        output_writer = self.output_writer
        background_cmds = tuple(self.session.background_cmds.update_and_get())
        if len(background_cmds) == 0:
            output_writer.print(
                '(No background cmds available)'
            )
            return
        output_writer.print('>>> Background commands:')
        for index, cmd in enumerate(background_cmds):
            output_writer.print(
                f'({index}) {str(cmd)}'
            )
    
    @inner_cmd_registry(key='ls-session')
    def list_session(self):
        output_writer = self.output_writer
        sessions = tuple(self.session.server.session_dict.values())
        if len(sessions) == 0:
            # This may never be executed.
            output_writer.print('(No sessions running)')
            return
        output_writer.print('>>> Sessions:')
        for index, session in enumerate(sessions):
            output_writer.print(f'({index}) {str(session)}')
    
    @inner_cmd_registry(key='ls-cmd')
    def list_cmd(self):
        output_writer = self.output_writer
        cmd_pool = self.session.cmd_pool
        with cmd_pool.queue_lock, cmd_pool.execute_lock:
            # Update the execute.
            cmd_pool.update_and_get_execute()
            output_writer.print('>>> Executing:')
            if len(cmd_pool.execute) == 0:
                output_writer.print('(No executing)')
            else:
                for index, cmd in enumerate(cmd_pool.execute):
                    output_writer.print(f'({index}) {str(cmd)}')
            output_writer.print('>>> Queued:')
            if len(cmd_pool.queue) == 0:
                output_writer.print('(No queued)')
            else:
                for index, cmd in enumerate(cmd_pool.queue):
                    output_writer.print(f'({index}) {str(cmd)}')

#
# Background Command
#

class BackgroundCommandList(
    BaseList[SessionCommand],
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass, InitOnceMetaclass)
):
    """
    Used to contain the commands that failed to terminate in time.
    """
    readonly_attr__ = ('lock',)
    
    def __init__(self):
        super().__init__()
        # Use a lock to make it safe.
        self.lock = RLock()
    
    def __setitem__(self, __key, __value: SessionCommand):
        with self.lock:
            super().__setitem__(__key, __value)
            self.update_command_states__()
    
    def __delitem__(self, __key):
        with self.lock:
            super().__delitem__(__key)
            self.update_command_states__()
    
    def insert(self, __index, __object: SessionCommand):
        with self.lock:
            super().insert(__index, __object)
            self.update_command_states__()

    def update_command_states__(self):
        with self.lock:
            self.set_list__(list(filter(
                lambda cmd: not cmd.cmd_state.exit.is_set(),
                self.get_list__()
            )))

    def update_and_get(self) -> "BackgroundCommandList":
        """
        Update command states and return self.
        """
        self.update_command_states__()
        return self
