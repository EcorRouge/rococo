import json
import os.path
from pprint import pprint
from typing import Any, Optional

from pydantic.v1 import BaseSettings, Extra

from .enums import SMSProvider


class Config(BaseSettings):
    CONFIG_FILEPATH: str
    SMS_PROVIDER: SMSProvider

    events: Any = None
    provider_config: Any = None

    class Config:
        extra = Extra.ignore

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.read_config()

    def read_config(self):
        if not os.path.isfile(self.CONFIG_FILEPATH):
            raise OSError(f'Config.json file not found on specified path. {self.CONFIG_FILEPATH}')

        with open(self.CONFIG_FILEPATH) as config:
            config = json.load(config)
            self.events = config.get('events')

            for configuration in config['configurations']:
                if configuration['provider'] == self.SMS_PROVIDER:
                    self.provider_config = configuration
                    pprint(self.provider_config)
                    if self.SMS_PROVIDER == SMSProvider.TWILIO:
                        # Ensure either 'senderPhoneNumber' or 'messagingServiceSid' is present
                        if not (self.provider_config.get('senderPhoneNumber') or self.provider_config.get('messagingServiceSid')):
                            raise ValueError('Missing required fields for Twilio configuration. At least one of "senderPhoneNumber" or "messagingServiceSid" must be provided.')

    def get_event(self, event_name: str):
        return self.events.get(event_name)

    @property
    def SENDER_PHONE_NUMBER(self) -> str:
        return self.provider_config.get("senderPhoneNumber")

    @property
    def MESSAGING_SERVICE_SID(self) -> str:
        return self.provider_config.get('messagingServiceSid')


class TwilioConfig(Config):
    TWILIO_ACCOUNT_SID: str
    TWILIO_AUTH_TOKEN: str


config_classes = [
    TwilioConfig
]


class SMSConfig(*config_classes):
    pass
