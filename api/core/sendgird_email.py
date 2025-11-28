import os
import asyncio
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from fastapi import HTTPException

async def send_email(to_email: str, pipeline_name: str, run_status: str, error_output: str) -> None:
    message = Mail(
    from_email = "noreply@orcestra.ca",
    to_emails = to_email,
    subject = "Sendgrid Email",
    html_content = "<strong>Run Status</strong>",
    )

    message.dynamic_template_data = {
        "pipeline_name": pipeline_name,
        "success": run_status,
        "error_output": error_output
    }

    message.template_id = os.getenv("SENDGRID_TEMPLATE_ID")
    try:
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        sg.send(message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending director email {str(e)}")
