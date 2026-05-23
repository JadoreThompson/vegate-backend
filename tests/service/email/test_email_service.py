import pytest
from config import CUSTOMER_SUPPORT_EMAIL
from module.email.brevo import BrevoEmailService


class TestBrevoEmailService:

    @pytest.fixture
    def email_service(self):
        return BrevoEmailService(
            sender_name="Test", sender_email="test@vegate.jadore.dev"
        )

    @pytest.mark.asyncio
    async def test_brevo_service_sends_successfully(self, email_service):
        await email_service.send_email(
            recipient=CUSTOMER_SUPPORT_EMAIL,
            subject="Testing brevo",
            body="Hello world",
        )
