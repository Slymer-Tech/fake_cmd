import os
import time
from itertools import chain
from argparse import ArgumentParser, Namespace
from slime_core.utils.typing import MISSING, Any, Missing, Sequence, Union
from . import config
from .logging import logger

#
# For-loop generator functions.
#


def polling(
    interval: Union[float, Missing] = MISSING, ignore_keyboard_interrupt: bool = False
):
    """
    Polling with given interval. If not specified,
    ``config.polling_interval`` is used.
    """
    interval = interval if interval is not MISSING else config.polling_interval
    while True:
        try:
            # NOTE: KeyboardInterrupt may be raised here and
            # it will propagate out of the for-loop if
            # ``ignore_keyboard_interrupt`` is False.
            time.sleep(interval)
        except KeyboardInterrupt:
            if not ignore_keyboard_interrupt:
                raise
        yield


def timeout_loop(
    timeout: Union[float, Missing],
    ignore_keyboard_interrupt: bool = False,
    interval: Union[float, Missing] = MISSING,
):
    """
    Forever executing the loop until timeout. If ``timeout`` is
    ``MISSING``, then it is equivalent to an infinite loop.
    """
    start = time.time()
    while True:
        try:
            # Check stop time.
            stop = time.time()
            if timeout is not MISSING and (stop - start) > timeout:
                break
        except KeyboardInterrupt:
            if not ignore_keyboard_interrupt:
                raise
        # Do something in the for-loop.
        yield
        try:
            # Sleep interval (if any).
            if interval is not MISSING:
                time.sleep(interval)
        except KeyboardInterrupt:
            if not ignore_keyboard_interrupt:
                raise


#
# Text services.
#


def timestamp_to_str(timestamp: float) -> str:
    """
    Parse timestamp to str.
    """
    time_tuple = time.localtime(int(timestamp))
    return time.strftime("%Y/%m/%d %H:%M:%S", time_tuple)


def get_server_name(address: str) -> str:
    dir_path = address
    possible_server_name = ""
    while dir_path and not possible_server_name:
        dir_path, possible_server_name = os.path.split(dir_path)

    if possible_server_name:
        return possible_server_name
    else:
        return "remote"


def get_control_char(char: str) -> str:
    """
    Get the corresponding ``Ctrl-*`` char with the given char.
    """
    char = char.lower()
    try:
        index = ord(char)
    except Exception as e:
        logger.error(str(e), stack_info=True)
        return ""

    if ord("a") <= index and index <= ord("z"):
        return chr(index - ord("a") + 1)
    # Other chars are not supported in this version (because they
    # are not used by the fake_cmd).
    return ""


#
# Logical operations.
#


def xor__(__x: Any, __y: Any) -> bool:
    return bool((__x and not __y) or (not __x and __y))


#
# Argument Parsers
#


class ArgNamespace(Namespace):

    def __bool__(self) -> bool:
        return True


def parser_parse(
    parser: ArgumentParser,
    args: Union[Sequence[str], Missing, None] = MISSING,
    strict: bool = True,
) -> Union[ArgNamespace, Missing]:
    """
    Argument parse args.
    """
    namespace = MISSING
    try:
        if args is MISSING:
            namespace = parser.parse_args(namespace=ArgNamespace())
        else:
            namespace = parser.parse_args(args=args, namespace=ArgNamespace())
    except SystemExit:
        if strict:
            raise
    return namespace


#
# Number comparison utils.
#


class GreaterThanAnything:
    """
    Used for comparison.
    """

    def __lt__(self, __value: Any) -> bool:
        return False

    def __le__(self, __value: Any) -> bool:
        return False

    def __eq__(self, __value: Any) -> bool:
        return False

    def __gt__(self, __value: Any) -> bool:
        return True

    def __ge__(self, __value: Any) -> bool:
        return True


class LessThanAnything:
    """
    Used for comparison.
    """

    def __lt__(self, __value: Any) -> bool:
        return True

    def __le__(self, __value: Any) -> bool:
        return True

    def __eq__(self, __value: Any) -> bool:
        return False

    def __gt__(self, __value: Any) -> bool:
        return False

    def __ge__(self, __value: Any) -> bool:
        return False


#
# UUID compression
#

BASE36_CHARS = "".join(chr(i) for i in chain(range(48, 58), range(97, 123)))
# Number of digits needed to represent a 32-bit hexadecimal number.
NUMBER_DIGITS = 25


def uuid_base36(number: int) -> str:
    """
    Convert a uuid to the base36 format.
    """
    base36_num = ""
    while number > 0:
        number, remainder = divmod(number, 36)
        base36_num = BASE36_CHARS[remainder] + base36_num

    return base36_num.rjust(NUMBER_DIGITS, "0")


#
# Inspect services.
#


def resolve_classname(obj: Any) -> str:
    """
    Try to resolve the classname of the given object.
    """
    cls = type(obj)
    candidate_classname_tuple = (
        getattr(cls, "__name__", None),
        getattr(cls, "__qualname__", None),
        str(cls),
    )
    for classname in candidate_classname_tuple:
        if classname:
            return classname
    return str(obj)
