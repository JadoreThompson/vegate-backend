import logging


class EmailService:
    _instances: dict[tuple[str, str], "EmailService"] = {}

    def __new__(cls, sender_name: str, sender_email: str):
        key = (sender_name, sender_email)
        if key in cls._instances:
            return cls._instances[key]
        instance = super().__new__(cls)
        cls._instances[key] = instance
        return instance

    def __init__(self, sender_name: str, sender_email: str) -> None:
        if hasattr(self, "_initialised") and self._initialised:
            return

        self._logger = logging.getLogger(type(self).__name__)
        self.sender_name = sender_name
        self.sender_email = sender_email
        self._initialised = True

    async def send_email(self, recipient: str, subject: str, body: str) -> None: ...

    def send_email_sync(self, recipient: str, subject: str, body: str) -> None: ...

    def close_sync(self): ...

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
