from typing import ClassVar

from .base import EmailService
from .brevo import BrevoEmailService
from .postmark import PostmarkEmailService
from .smtpgo import SmtpgoEmailService


class EmailServiceFactory:

    _services: ClassVar[dict[tuple[str, str, str], EmailService]] = {}

    @classmethod
    def create(
        cls,
        provider: str,
        sender_name: str,
        sender_email: str,
    ) -> EmailService:
        key = (provider, sender_name, sender_email)

        if key in cls._services:
            return cls._services[key]

        if provider == "brevo":
            service = BrevoEmailService(
                sender_name=sender_name,
                sender_email=sender_email,
            )
        elif provider == "postmark":
            service = PostmarkEmailService(
                sender_name=sender_name,
                sender_email=sender_email,
            )
        elif provider == "smtpgo":
            service = SmtpgoEmailService(
                sender_name=sender_name,
                sender_email=sender_email,
            )
        else:
            raise ValueError(f"Email provider '{provider}' not supported")

        cls._services[key] = service
        return service

    @classmethod
    async def close_all(cls) -> None:
        for service in cls._services.values():
            if not service.closed:
                await service.close()

        cls._services.clear()