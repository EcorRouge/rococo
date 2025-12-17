from typing import Any

from .base import EmailService
from .config import SESConfig


class SESService(EmailService):
    """AWS SES email service implementation stub."""

    def __init__(self):
        # No initialization required - configuration is set via __call__
        pass

    def __call__(self, config: SESConfig, *args, **kwargs):
        super().__call__(config)

        return self

    def send_email(self, message: dict) -> Any:
        # SES send_email implementation pending - requires AWS SES setup
        pass

    def create_contact(self, email: str, name: str, list_id: str, extra: dict):
        # SES create_contact implementation pending - requires AWS SES setup
        pass

    def remove_contact(self, email: str):
        # SES remove_contact implementation pending - requires AWS SES setup
        pass
