import asyncio


class RateLimiter:
    def __init__(self, max_requests: int, per_seconds: int):
        self._max_requests = max_requests
        self._per_seconds = per_seconds
        self._window_start = asyncio.get_event_loop().time()
        self._count = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()

                if now - self._window_start >= self._per_seconds:
                    self._window_start = now
                    self._count = 0

                if self._count < self._max_requests:
                    self._count += 1
                    return
                else:
                    sleep_duration = self._per_seconds - (now - self._window_start)

            await asyncio.sleep(sleep_duration)
