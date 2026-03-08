import time
import signal
from contextlib import contextmanager
from functools import partial, wraps
from slime_core.utils.base import BaseList
from slime_core.utils.typing import (
    MISSING,
    Callable,
    Missing,
    Type,
    Callable,
    TypeVar,
    Union,
    Tuple,
)
from . import config
from .logging import logger

_T = TypeVar("_T")
_CallableT = TypeVar("_CallableT", bound=Callable)


class CLITerminate(Exception):
    pass


class ServerShutdown(Exception):
    pass


#
# Exception handling.
#


class ExceptionInfo(BaseList[BaseException]):
    """
    Used to check the exception information.
    """

    pass


def retry(
    func: Callable[[], _T],
    suppress_exc: Union[
        Type[BaseException], Tuple[Type[BaseException], ...]
    ] = Exception,
    max_retries: Union[int, Missing] = MISSING,
    exception_wait_timeout: Union[float, Missing] = MISSING,
) -> Union[_T, ExceptionInfo]:
    """
    Try to call the ``func`` and retry.
    """
    max_retries = (
        max_retries if max_retries is not MISSING else config.exception_retries
    )
    exception_wait_timeout = (
        exception_wait_timeout
        if exception_wait_timeout is not MISSING
        else config.exception_wait_timeout
    )

    info = ExceptionInfo()
    attempt = 0
    while True:
        try:
            return func()
        except suppress_exc as e:
            info.append(e)

        if attempt >= max_retries:
            logger.warning(
                f"Func {str(func)} run {attempt} times, but "
                "still failed, returning the ExceptionInfo."
            )
            return info

        attempt += 1
        logger.warning(
            f"Exception occurred in {str(func)}, retry after "
            f"{exception_wait_timeout} seconds. Exception: {str(info)}."
        )
        time.sleep(exception_wait_timeout)


def retry_deco(
    suppress_exc: Union[
        Type[BaseException], Tuple[Type[BaseException], ...]
    ] = Exception,
    max_retries: Union[int, Missing] = MISSING,
    exception_wait_timeout: Union[float, Missing] = MISSING,
) -> Callable[[_CallableT], _CallableT]:
    """
    Decorator version of retry.
    """

    def decorator(func: _CallableT) -> _CallableT:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return retry(
                partial(func, *args, **kwargs),
                suppress_exc=suppress_exc,
                max_retries=max_retries,
                exception_wait_timeout=exception_wait_timeout,
            )

        return wrapper

    return decorator


@contextmanager
def ignore_keyboard_interrupt():
    """
    Ignoring keyboard interrupt in the block. Can only be used in
    the main thread of the main interpreter.
    """
    sigint = signal.SIGINT
    previous = signal.getsignal(sigint)
    signal.signal(sigint, lambda *args, **kwargs: None)
    try:
        yield
    finally:
        signal.signal(sigint, previous)
