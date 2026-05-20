from typing import Protocol, runtime_checkable


@runtime_checkable
class Closeable(Protocol):

    def close(self): ...


@runtime_checkable
class AsyncCloseable(Protocol):

    async def close(self): ...
