from typing import Any

from .base import EmailService
from .config import SESConfig


class SESService(EmailService):
    """AWS SES Email Service implementation - currently a stub for future implementation."""

    def __init__(self):
        # No initialization needed for SES stub
        pass

    def __call__(self, config: SESConfig, *args, **kwargs):
        super().__call__(config)

        return self

    def send_email(self, message: dict) -> Any:
        # TODO: Implement SES send_email functionality
        pass

    def create_contact(self, email: str, name: str, list_id: str, extra: dict):
        # TODO: Implement SES create_contact functionality
        pass

    def remove_contact(self, email: str):
        # TODO: Implement SES remove_contact functionality
        pass
