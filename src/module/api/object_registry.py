from typing import Type, TypeVar

# from lib.protocol import Closeable, AsyncCloseable
from core.protocol import Closeable, AsyncCloseable

T = TypeVar("T")


class ObjectRegistry:

    def __init__(self):
        self._registry: dict = {}
        pass

    def register(self, obj: object):
        self._registry[obj.__class__] = obj

    def get(self, clazz: Type[T], subclass=False) -> T:
        if subclass:
            for C in clazz.__subclasses__():
                obj = self._registry.get(C)
                if obj:
                    return obj
            raise KeyError(f"Subclass of type {clazz} not found")
        # print(self._registry)
        return self._registry[clazz]

    async def close(self):
        for obj in self._registry.values():
            if isinstance(obj, AsyncCloseable):
                await obj.close()
            elif isinstance(obj, Closeable):
                obj.close()
        self._registry.clear()
