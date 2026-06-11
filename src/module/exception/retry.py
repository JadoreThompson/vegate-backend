import asyncio
import functools
import inspect
import logging
import time


class Retry:
    """
    Wraps a function with retry logic for specified exceptions.

    Supports both sync and async functions, retrying up to `retry_attempts`
    with a fixed `retry_delay`. Only configured exceptions are retried;
    all others propagate immediately.
    """

    def __init__(
        self,
        *,
        exceptions: list[type[Exception]],
        retry_attempts: int = 5,
        retry_delay: float = 0.5,
    ):
        """
        Args:
            exceptions (list[type[Exception]]):
                A list of exception types that should trigger a retry when raised
                by the wrapped function.
            retry_attempts (int, optional):
                Maximum number of attempts before giving up. Defaults to 5.
            retry_delay (float, optional):
                Delay in seconds between retry attempts. Defaults to 0.5.
        """
        self._exceptions = tuple(exceptions)
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay
        self._logger = logging.getLogger(__name__)

    def __call__(self, func):
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def wrapper(*args, **kw):
                for attempt in range(self._retry_attempts):
                    try:
                        return await func(*args, **kw)
                    except self._exceptions as e:
                        if attempt + 1 == self._retry_attempts:
                            raise
                        self._logger.info(
                            f"Attempt {attempt + 1} failed: {e}, retrying in {self._retry_delay}s..."
                        )
                        await asyncio.sleep(self._retry_delay)
                    except Exception as e:
                        raise e

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                for attempt in range(self._retry_attempts):
                    try:
                        return func(*args, **kwargs)
                    except self._exceptions as e:
                        if attempt + 1 == self._retry_attempts:
                            raise
                        self._logger.info(
                            f"Attempt {attempt + 1} failed: {e}, retrying in {self._retry_delay}s..."
                        )
                        time.sleep(self._retry_delay)
                    except Exception as e:
                        raise e

        return wrapper

