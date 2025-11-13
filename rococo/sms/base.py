from abc import ABC, abstractmethod
from typing import Any

from .config import SMSConfig


class SMSService(ABC):
    """
        Base class for SMS service providers
    """
    config: SMSConfig

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, config: SMSConfig, *args, **kwargs):
        self.config = config

    @abstractmethod
    def send_sms(self, event_name: str, phone_number: str, parameters: dict) -> Any:
        """
        Sends an SMS message
        """
        raise NotImplementedError
