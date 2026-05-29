from config import SMTPGO_API_KEY
from .base import EmailService
from .exception import EmailServiceException


class SmtpgoEmailService(EmailService):

    def __init__(self, sender_name, sender_email, *, api_key: str = SMTPGO_API_KEY):
        super().__init__(sender_name, sender_email)
        self._api_key = api_key
        self._url = "https://api.smtp2go.com/v3/email/send"
        self._headers = {
            "X-Smtp2go-Api-Key": self._api_key,
            "Content-Type": "application/json",
        }

    async def send_email(self, recipient: str, subject: str, body: str) -> None:
        self._ensure_open()

        if not recipient:
            raise ValueError("recipient is required")

        await self._send_via_smtpgo(recipient, subject, body)
        self._log_sent(recipient)

    async def _send_via_smtpgo(self, recipient: str, subject: str, body: str) -> None:
        await self._ensure_http_sess()

        payload = self._build_body(recipient, subject, body)

        rsp = await self._http_sess.post(self._url, json=payload, headers=self._headers)
        if rsp.status >= 400:
            text = await rsp.text()
            msg = f"SMTPGO API error: {rsp.status} - {text}"
            self._logger.error(msg)
            raise EmailServiceException(msg)

    async def close(self):
        """Gracefully close the internal HTTP session."""
        if self._closed:
            return

        self._closed = True
        if self._http_sess is not None and not self._http_sess.closed:
            await self._http_sess.close()

    def _build_body(self, recipient: str, subject: str, body: str) -> dict:
        return {
            "sender": self._sender_email,
            "to": [recipient],
            "subject": subject,
            "text_body": body,
            "fastaccept": True,
        }

    def _log_sent(self, recipient: str):
        msg = (
            f"Email sent via SMTPGO sender_name={self._sender_name}, "
            f"sender_email={self._sender_email}, recipient={recipient}"
        )
        self._logger.info(msg)
