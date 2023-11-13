from typing import Any

from .base import EmailService
from .config import SESConfig


class SESService(EmailService):

    def __init__(self):
        pass

    def __call__(self, config: SESConfig, *args, **kwargs):
        super().__call__(config)

        return self

    def send_email(self, message: dict) -> Any:
        pass
