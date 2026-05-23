from fastapi import APIRouter, BackgroundTasks, status

from config import CUSTOMER_SUPPORT_EMAIL
from module.email import BrevoEmailService
from .schema import ContactForm


router = APIRouter(prefix="/contact", tags=["Public"])
email_service = BrevoEmailService("Vegate", "support@jadore.dev")


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def contact_us(body: ContactForm, background_tasks: BackgroundTasks):
    subject = f"New Contact Inquiry from {body.name}"

    em_body = f"""
You have received a new message from the Gova website contact form.\n

Contact Details:\n
- Name: {body.name}\n
- Email: {body.email}\n

Message:\n
{body.message}
"""
    background_tasks.add_task(
        email_service.send_email,
        recipient=CUSTOMER_SUPPORT_EMAIL,
        subject=subject,
        body=em_body.strip(),
    )
