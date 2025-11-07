import abc
import asyncio
from typing import Any, Callable, ParamSpec
from uuid import uuid4

P = ParamSpec("P")


class BackgroundTasks(abc.ABC):
    def __init__(self, tasks: set[asyncio.Task[Any]] | None = None):
        self._tasks: set[asyncio.Task[Any]] = (
            tasks if tasks is not None else set()
        )

    @property
    def size(self):
        return len([t for t in self._tasks if self.is_running(t)])

    def is_running(self, task: asyncio.Task[Any]):
        if not task.done():
            return True
        return False

    def add(self, coro: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
        """Start a background task and add it to the tracked tasks.

        When the task finishes, it is automatically cleaned up."""
        task_name = str(uuid4())
        task = asyncio.get_running_loop().create_task(
            coro(*args, **kwargs), name=task_name
        )
        task.add_done_callback(self._tasks.discard)
        self._tasks.add(task)

    async def wait(self, timeout: int = 10):
        await asyncio.wait_for(asyncio.gather(*self._tasks), timeout=timeout)
