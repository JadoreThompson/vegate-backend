import logging
from abc import ABC, abstractmethod

import aiohttp

from .exceptions import ClosedEmailServiceException


class BaseEmailService(ABC):
    _instances: dict[tuple[str, str], "BaseEmailService"] = {}

    def __new__(cls, sender_name: str, sender_email: str, *args, **kw):
        key = (sender_name, sender_email)
        if key in cls._instances:
            return cls._instances[key]
        instance = super().__new__(cls)
        cls._instances[key] = instance
        return instance

    def __init__(self, sender_name: str, sender_email: str) -> None:
        if getattr(self, "_initialised", False):
            return

        self._sender_name = sender_name
        self._sender_email = sender_email
        self._http_sess: aiohttp.ClientSession | None = None

        self._initialised = True
        self._closed = False

        cls_name = type(self).__name__
        self._logger = logging.getLogger(f"{cls_name}-{sender_email}")
        self._logger.debug(f"Logger for {sender_email} initialised.")

    @property
    def sender_email(self):
        return self._sender_email

    @property
    def sender_name(self):
        return self._sender_name

    @property
    def closed(self):
        return self._closed

    @abstractmethod
    async def send_email(self, recipient: str, subject: str, body: str) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @classmethod
    async def close_all(cls):
        for service in list(cls._instances.values()):
            if not service._closed:
                await service.close()

    async def _ensure_http_sess(self) -> None:
        if self._http_sess is None or self._http_sess.closed:
            self._http_sess = aiohttp.ClientSession()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ClosedEmailServiceException()

    @staticmethod
    def _escape_html(s: str) -> str:
        """Minimal HTML escaper for embedding plain text into <pre> blocks."""
        return (
            s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;")
        )
