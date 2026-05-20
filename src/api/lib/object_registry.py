from typing import Type

from lib.protocol import Closeable, AsyncCloseable


class ObjectRegistry:
    
    _registry: dict = {}

    @classmethod
    def register(cls, obj: object):
        cls._registry[obj.__class__] = obj

    @classmethod
    def get(cls, clazz: Type) -> object:
        return cls._registry[clazz]

    @classmethod
    async def close(cls):
        for obj in cls._registry.values():
            if isinstance(obj, AsyncCloseable):
                await obj.close()
            elif isinstance(obj, Closeable):
                obj.close()
