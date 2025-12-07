from aiohttp import ClientSession


class HTTPSessMixin:
    _http_sess: ClientSession | None = None
    _initialised = False

    @classmethod
    def initialise(cls):
        if cls._initialised:
            raise ValueError(f"{cls.__name__} has already been initialised")

        cls._http_sess = ClientSession()

    @classmethod
    async def cleanup(cls):
        if cls._http_sess is not None and not cls._http_sess.closed():
            await cls._http_sess.close()
