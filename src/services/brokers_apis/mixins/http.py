from aiohttp import ClientSession


class HTTPSessMixin:
    _http_sess: ClientSession | None = None

    def __init__(self, *args, **kw):
        """Initialize the HTTP session automatically."""
        super().__init__(*args, **kw)
        if self._http_sess is None or self._http_sess.closed:
            self.__class__._http_sess = ClientSession()

    @property
    def http_session(self) -> ClientSession:
        """Property to ensure HTTP session is always available."""
        if self._http_sess is None or self._http_sess.closed:
            self.__class__._http_sess = ClientSession()
        return self._http_sess

    @classmethod
    async def cleanup(cls):
        """Close the HTTP session if it exists."""
        if cls._http_sess is not None and not cls._http_sess.closed:
            await cls._http_sess.close()
