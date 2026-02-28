import contextlib
import sys
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Sequence
from typing import Generic, TypeVar

import anyio
from anyio.abc import TaskStatus

from .errors import RaceFailedError

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

if sys.version_info >= (3, 13):
    from typing import TypeVarTuple
else:
    from typing_extensions import TypeVarTuple


_T = TypeVar("_T")
# defines a single argument function
_S = TypeVar("_S")
_Ts = TypeVarTuple("_Ts", default=Unpack[tuple[()]])

SENTINAL = object()


class Racer(Generic[_T, Unpack[_Ts]]):
    """Provides an asynchronous interface for racing & brute-forcing tasks
    multiple times until a single task succeededs"""

    def __init__(
        self,
        func: Callable[[Unpack[_Ts]], Awaitable[_T]],
        delay: float | None = None,
        task_delay: float | None = None,
        max_tasks: int | None = None,
        suppress: Sequence[type[Exception]] | None = None,
        # exc_handler: Callable[[BaseException], bool] | None = None
    ):
        """
        :param func: An asynchronous function to attempt to brute-force for.
        :type func: Callable[[Unpack[_Ts]], Awaitable[_T]]
        :param delay: When the race should be backed out.
        :type delay: float | None
        :param task_dealy: when the task should be considered as failing.
        :type task_dealy: float | None
        :param max_tasks: maximum number of tasks to be ran concurrently, defaults to 32
            to prevent wasting resources.
        :param suppress: a list of exceptions that should be slienced. Useful when
            brute-forcing different connections.
        :param exc_handler: an alternative to suppress for handling exceptions
        :type exc_handler: Callable[[BaseException], bool] | None
        """

        self._func = func

        self._delay = delay
        # deadline to wait on all tasks to complete for.
        self._cancel_scope: anyio.CancelScope | None = None
        # The safely killable scope for when an object is seen succeeding
        self._runner_scope: anyio.CancelScope | None = None
        self._task_delay = task_delay
        self._limiter = anyio.CapacityLimiter(max_tasks or 32)
        # self._handler = Handler(exc_handler, suppress)
        self._suppress = suppress or tuple()
        # its just here to prevent the limiter and scopes from being abused.
        # if you want the runner to reamin concurrent my suggestion is using the
        # public race and arace functions instead.
        self._lock = anyio.Lock()

        self._result = SENTINAL

    @contextlib.contextmanager
    def _cancel_scope_factory(self):
        with anyio.fail_after(delay=self._delay) as self._cancel_scope:
            yield

    @contextlib.contextmanager
    def _runner_scope_factory(self):
        with anyio.move_on_after(None) as self._runner_scope:
            yield

    async def _run_task(
        self,
        args: tuple[Unpack[_Ts]],
        task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ):
        async with self._limiter:
            # label as started if we reach inside the limiter so that we aren't flooding
            # the race incase objects are infinate.
            # Example: 20GB file of text with each line seen as something to do.
            task_status.started()
            # callback is used incase exception is raised which then
            # that is a loss and should not be counted as a winning task.
            with anyio.move_on_after(self._task_delay):
                # BUG: Currently there is no way to safely suppress tasks that get cancelled from a certain scope
                # which means that all of these wind up suppressed.
                try:
                    with contextlib.suppress(*self._suppress):
                        ret = await self._func(*args)

                        # safely exit from the runner's scope when an object is found.
                        self._result = ret
                        if not self.group.cancel_scope.cancel_called:
                            self.group.cancel_scope.cancel()

                except anyio.get_cancelled_exc_class() as exc:
                    # throw only when a winner is not found.
                    if self._result == SENTINAL:
                        raise exc

    async def run(
        self, it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]]
    ) -> _T:
        """Runs multiple tasks until a single task has been seen finishing,
        any suppressed errors that a user specifies will not be thrown unless improperly handled.

        :param it: an interable object to iterate over, this can as infinate as the user desires.
        :type it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]]

        :raises TimeoutError: if inital deadline for the racer expires
        :raises RaceFailedError: if no task suceeded.
        """
        async with self._lock:
            with self._cancel_scope_factory():
                with self._runner_scope_factory():
                    # group scope is placed inside so that anything left running will safely
                    # be allowed to finish until a single task suceeds which then it can terminate.
                    async with anyio.create_task_group() as self.group:
                        for i in it:
                            await self.group.start(self._run_task, i)
            if self._result == SENTINAL:
                raise RaceFailedError("All tasks failed")
            else:
                ret = self._result
                self._result = SENTINAL
                return ret

    async def arun(self, ait: AsyncIterable[tuple[Unpack[_Ts]]]) -> _T:
        """Same as run(...) but can take an infinate number of asynchronous iterables along with it

        :param it: an ansynchonous interable object to iterate over, this can as infinate as the user desires.
        :type it: AsyncIterable[tuple[Unpack[_Ts]]]

        :raises TimeoutError: if inital deadline for the racer expires
        :raises RaceFailedError: if no task suceeded.
        """
        # prevent inner scopes from being screwed around with.
        async with self._lock:
            with self._cancel_scope_factory():
                with self._runner_scope_factory():
                    async with anyio.create_task_group() as self.group:
                        async for i in ait:
                            await self.group.start(self._run_task, *i)

            if self._result == SENTINAL:
                raise RaceFailedError("All tasks failed")
            else:
                ret = self._result
                self._result = SENTINAL
                return ret


async def race(
    func: Callable[[Unpack[_Ts]], Awaitable[_T]],
    it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]],
    delay: float | None = None,
    task_delay: float | None = None,
    max_tasks: int | None = None,
    suppress: Sequence[type[Exception]] | None = None,
):
    """Provides an asynchronous brute-forcing mechanism for numerous tasks,
    if a task succeeds it is returned

    :param func: An asynchronous function to attempt to brute-force for.
    :type func: Callable[[Unpack[_Ts]], Awaitable[_T]]
    :param it: an interable object to iterate over, this can as infinate as the user desires.
    :type it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]]
    :param delay: When the race should be backed out.
    :type delay: float | None
    :param task_dealy: when the task should be considered as failing.
    :type task_dealy: float | None
    :param max_tasks: maximum number of tasks to be ran concurrently, defaults to 32
        to prevent wasting resources.
    :param suppress: a list of exceptions that should be slienced. Useful when
        brute-forcing different connections.
    :param exc_handler: an alternative to suppress for handling exceptions
    :type exc_handler: Callable[[BaseException], bool] | None

    :raises TimeoutError: if inital deadline for the racer expires
    :raises RaceFailedError: if no task suceeded.

    :returns: the successful task's return object.
    """
    return await Racer(func, delay, task_delay, max_tasks, suppress).run(it)


async def race_single(
    func: Callable[[_S], Awaitable[_T]],
    it: Iterable[_S] | Sequence[_S],
    delay: float | None = None,
    task_delay: float | None = None,
    max_tasks: int | None = None,
    suppress: Sequence[type[Exception]] | None = None,
) -> _T:
    """races a single argument function instaead of needing to pack multiple tuples.

    :param func: An asynchronous function to attempt to brute-force for.
    :type func: Callable[[Unpack[_Ts]], Awaitable[_T]]
    :param it: an interable object to iterate over, this can as infinate as the user desires.
    :type it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]]
    :param delay: When the race should be backed out.
    :type delay: float | None
    :param task_dealy: when the task should be considered as failing.
    :type task_dealy: float | None
    :param max_tasks: maximum number of tasks to be ran concurrently, defaults to 32
        to prevent wasting resources.
    :param suppress: a list of exceptions that should be slienced. Useful when
        brute-forcing different connections.
    :param exc_handler: an alternative to suppress for handling exceptions
    :type exc_handler: Callable[[BaseException], bool] | None

    :raises TimeoutError: if inital deadline for the racer expires
    :raises RaceFailedError: if no task suceeded.

    :returns: the successful task's return object.
    """

    return await race(func, map(tuple, it), delay, task_delay, max_tasks, suppress)


async def arace(
    func: Callable[[Unpack[_Ts]], Awaitable[_T]],
    it: AsyncIterable[tuple[Unpack[_Ts]]],
    delay: float | None = None,
    task_delay: float | None = None,
    max_tasks: int | None = None,
    suppress: Sequence[type[Exception]] | None = None,
):
    """Provides an asynchronous brute-forcing mechanism for numerous tasks for asynchronous iterables,
    if a task succeeds it is returned

    :param func: An asynchronous function to attempt to brute-force for.
    :type func: Callable[[Unpack[_Ts]], Awaitable[_T]]
    :param it: an interable object to iterate over, this can as infinate as the user desires.
    :type it: Iterable[tuple[Unpack[_Ts]]] | Sequence[tuple[Unpack[_Ts]]]
    :param delay: When the race should be backed out.
    :type delay: float | None
    :param task_dealy: when the task should be considered as failing.
    :type task_dealy: float | None
    :param max_tasks: maximum number of tasks to be ran concurrently, defaults to 32
        to prevent wasting resources.
    :param suppress: a list of exceptions that should be slienced. Useful when
        brute-forcing different connections.
    :param exc_handler: an alternative to suppress for handling exceptions
    :type exc_handler: Callable[[BaseException], bool] | None

    :returns: the successful task's return object.

    :raises TimeoutError: if inital deadline for the racer expires
    :raises RaceFailedError: if no task suceeded.
    """
    return await Racer(func, delay, task_delay, max_tasks, suppress).run(it)


__all__ = ("Racer", "arace", "race", "race_single")
