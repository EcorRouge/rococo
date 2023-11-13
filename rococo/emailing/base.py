from abc import ABC, abstractmethod
from typing import Any

from .config import EmailConfig


class EmailService(ABC):
    """
        Base class for email service providers
    """
    config: EmailConfig

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, config: EmailConfig, *args, **kwargs):
        self.config = config

    @abstractmethod
    def send_email(self, message: dict) -> Any:
        return NotImplementedError
