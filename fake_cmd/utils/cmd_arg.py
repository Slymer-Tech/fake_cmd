"""
Parse client command arguments.
"""

import shlex
from argparse import ArgumentParser
from abc import ABC, abstractmethod
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Union,
    Tuple,
    List,
    Missing,
    Literal,
    MISSING,
    Sequence,
    Type,
)
from . import config
from .common import ArgNamespace, parser_parse
from .logging import logger
from .executors import platform_open_executor_registry
from .executors.writer import popen_writer_registry
from .executors.reader import popen_reader_registry, pexpect_reader_registry

#
# Split command for further analysis.
#

# Separator to separate between command options and session command.
CMD_SEPARATOR = " -- "
# Command split type.
CMDSplit = Tuple[Union[List[str], Missing], Union[str, Missing]]


def split_cmd(cmd_str: str) -> Union[CMDSplit, Literal[False]]:
    """
    Split the command str into possible command options and possible
    session command.

    Example:
    ```Python
    # (['inter'], 'python -i')
    split_cmd('inter -- python -i')
    # (['inter', '--writer', 'pty'], MISSING<0x7fa1282a4310>)
    split_cmd('inter --writer pty')
    # (['inter', '--exec', '/path/with white space', '--writer', 'pty'], 'python -i')
    split_cmd('inter --exec "/path/with white space" --writer pty -- python -i')
    # (MISSING<id>, 'a')
    split_cmd(' -- a')
    # (['a'], '')
    split_cmd('a -- ')
    # (MISSING<id>, '')
    split_cmd(' -- ')
    ```
    """
    cmd_splits = cmd_str.split(CMD_SEPARATOR, 1)
    split_len = len(cmd_splits)
    if split_len == 0:
        return False
    elif split_len > 2:
        logger.warning(
            "Expect the length of ``cmd_splits`` to be no more than 2, "
            f"but {split_len} found. Split result: {cmd_splits}"
        )
        return False

    # Split the possible command options for further analyzing.
    shlex_splits = shlex.split(cmd_splits[0], posix=config.posix_shlex)
    if len(shlex_splits) == 0:
        shlex_splits = MISSING
    if split_len == 1:
        return shlex_splits, MISSING
    else:
        return shlex_splits, cmd_splits[1]


#
# ArgumentParsers.
#


class CMDParser(ABC):

    @abstractmethod
    def get_parser(self) -> ArgumentParser:
        """
        Return an ArgumentParser object.
        """
        pass

    @abstractmethod
    def set_additional_args(self, args: ArgNamespace) -> None:
        """
        Set additional command args to the namespace.
        """
        pass

    @abstractmethod
    def make_cmd_content(self, args: ArgNamespace) -> dict:
        """
        Make command content dict using args.
        """
        pass

    def parse_args(
        self, args: Union[Sequence[str], Missing, None] = MISSING, strict: bool = False
    ) -> Union[ArgNamespace, Missing]:
        """
        Parse arguments using ArgumentParser, and set additional
        args.
        """
        parser = self.get_parser()
        args = parser_parse(parser, args, strict)
        if args is MISSING:
            # If ``args`` is ``MISSING``, directly return.
            return args
        # Set additional args.
        self.set_additional_args(args)
        return self.check_args(args)

    def parse_cmd(
        self, cmd_splits: CMDSplit, strict: bool
    ) -> Union[ArgNamespace, Literal[False]]:
        possible_options, possible_cmd = cmd_splits
        if possible_options is MISSING:
            return False

        args = self.parse_args(args=possible_options[1:], strict=strict)
        if args is MISSING or possible_cmd is MISSING:
            return False

        args.cmd = possible_cmd
        return args

    def check_args(self, args: ArgNamespace) -> Union[ArgNamespace, Missing]:
        """
        Check whether the args are compatible, and return ``MISSING`` if
        they are not.
        """
        return args


cmd_parser_registry = Registry[Type[CMDParser]]("cmd_parser_registry")


class PopenCMDParser(CMDParser):

    @abstractmethod
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser()
        parser.add_argument(
            "--encoding",
            default=None,
            type=str,
            required=False,
            help=(
                "The encoding method of input and output. Should not "
                "be specified most of the time (and leave it default)."
            ),
        )
        parser.add_argument(
            "--exec",
            default=None,
            type=str,
            required=False,
            help=(
                "The executable script path. Should not be specified "
                "most of the time (and leave it default)."
            ),
        )
        parser.add_argument(
            "--platform",
            default=None,
            type=str,
            required=False,
            help=(
                "The running platform that decides process operations. "
                "Should not be specified most of the time (and leave it "
                "default)."
            ),
            choices=platform_open_executor_registry.keys(),
        )
        parser.add_argument(
            "--reader",
            default="default",
            type=str,
            choices=popen_reader_registry.keys(),
            required=False,
            help=(
                "The output redirection setting. Specify the reader "
                "setting of Popen."
            ),
        )
        return parser

    def make_cmd_content(self, args: ArgNamespace) -> dict:
        return {
            "cmd": args.cmd,
            "encoding": args.encoding,
            "reader": args.reader,
            "writer": args.writer,
            "exec": args.exec,
            "platform": args.platform,
            "interactive": args.interactive,
            "kill_disabled": args.kill_disabled,
            # Add 'stdin' content for backward compatibility.
            "stdin": args.writer,
        }


@cmd_parser_registry(key="cmd")
class PlainPopenParser(PopenCMDParser):

    def get_parser(self) -> ArgumentParser:
        parser = super().get_parser()
        parser.prog = "cmd"
        return parser

    def set_additional_args(self, args: ArgNamespace) -> None:
        args.interactive = False
        args.kill_disabled = False
        args.writer = None


@cmd_parser_registry(key="inter")
class InterPopenParser(PopenCMDParser):

    def get_parser(self) -> ArgumentParser:
        parser = super().get_parser()
        parser.prog = "inter"
        # NOTE: ``default`` is an excluded option in the interactive mode because it
        # is not writable through fake_cmd.
        excluded_writers = set(["default"])
        writer_choices = set(popen_writer_registry.keys()).difference(excluded_writers)
        parser.add_argument(
            "--writer",
            # Set the default to 'pipe'.
            default="pipe",
            type=str,
            choices=writer_choices,
            required=False,
            help=(
                "The interactive input setting. Specify the writer " "setting of Popen."
            ),
        )
        parser.add_argument(
            "--kill_disabled",
            action="store_true",
            help=(
                "Whether keyboard interrupt is disabled to kill the command. NOTE: This "
                "option is not encouraged in ``inter``. If you are running the server on "
                "a unix system, installing and using ``pexpect`` is recommended."
            ),
        )
        return parser

    def set_additional_args(self, args: ArgNamespace) -> None:
        args.interactive = True

    def check_args(self, args: ArgNamespace) -> Union[ArgNamespace, Missing]:
        args = super().check_args(args)
        if args is MISSING:
            return args
        if args.kill_disabled:
            # NOTE: ``kill_disabled`` is not an encouraged option
            # in the ``inter`` running mode.
            if args.writer in ("pty",):
                logger.warning(
                    f"``kill_disabled`` is not supported when --writer is ``{args.writer}``."
                )
                return MISSING
            logger.warning(
                "You are setting ``kill_disabled`` to True in the interactive mode, and this "
                "is not perfectly supported. For example, the command may not be successfully "
                "terminated if you open a /bin/bash shell console and run a command in it. "
                "If you are starting the server on a Unix system, install and use ``pexpect`` "
                "instead to get better user experience."
            )
        # Check passed.
        return args


@cmd_parser_registry(key="pexpect")
class PexpectParser(CMDParser):

    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser(prog="pexpect")
        parser.add_argument(
            "--reader",
            default="default",
            type=str,
            choices=pexpect_reader_registry.keys(),
            required=False,
            help=(
                "The output redirection setting. Specify the reader "
                "setting of pexpect."
            ),
        )
        parser.add_argument(
            "--encoding",
            default=None,
            type=str,
            required=False,
            help=(
                "The encoding method of input and output. Should not "
                "be specified most of the time (and leave it default)."
            ),
        )
        parser.add_argument(
            "--kill_disabled",
            action="store_true",
            help=("Whether keyboard interrupt is disabled to kill the command."),
        )
        parser.add_argument(
            "--echo",
            action="store_true",
            help=("Whether enable echo mode (the input will be displayed again)."),
        )
        return parser

    def set_additional_args(self, args: ArgNamespace) -> None:
        args.interactive = True

    def make_cmd_content(self, args: ArgNamespace) -> dict:
        return {
            "cmd": args.cmd,
            "encoding": args.encoding,
            "reader": args.reader,
            "interactive": args.interactive,
            "kill_disabled": args.kill_disabled,
            "echo": args.echo,
        }
