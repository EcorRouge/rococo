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
        """
        Sends an email message
        """
        raise NotImplementedError

    @abstractmethod
    def create_contact(self, email: str, name: str, list_id: str, extra: dict):
        """
        Creates a contact using specified email and name.
        """
        raise NotImplementedError

    @abstractmethod
    def remove_contact(self, email: str):
        """
        Removes a contact with specified email
        """
        raise NotImplementedError
