"""Config class"""
import json
import os.path
from pprint import pprint
from typing import Any

from pydantic import BaseSettings, Extra

from .enums import EmailProvider


class Config(BaseSettings):
    """Extends base settings for configuration"""
    CONFIG_FILEPATH: str
    EMAIL_PROVIDER: EmailProvider

    events: Any = None
    provider_config: Any = None

    class Config: # pylint: disable=R0903
        """Config class"""
        extra = Extra.ignore

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.read_config()

    def read_config(self):
        """Read email configuration file"""
        if not os.path.isfile(self.CONFIG_FILEPATH):
            raise OSError(f'Config.json file not found on specified path. {self.CONFIG_FILEPATH}')

        with open(self.CONFIG_FILEPATH,encoding='UTF-8') as config:
            config = json.load(config)
            self.events = config.get('events')

            for configuration in config['configurations']:
                if configuration['provider'] == self.EMAIL_PROVIDER:
                    self.provider_config = configuration
                    pprint(self.provider_config)

    def get_event(self, event_name: str):
        """Gets the event"""
        return self.events.get(event_name)

    @property
    def SOURCE_EMAIL(self) -> str:
        """Returns configured sourceEmail value"""
        return self.provider_config.get("sourceEmail")

    @property
    def ERROR_REPORTING_EMAIL(self) -> str:
        """Returns configured errorReportingEmail value"""
        return self.provider_config.get('errorReportingEmail')


class MailjetConfig(Config):
    """Mailjet Configuration keys"""
    MAILJET_API_KEY: str
    MAILJET_API_SECRET: str
    MAILJET_API_VERSION: str = 'v3.1'


class SESConfig(Config):
    """SES Configuration keys"""
    pass # pylint: disable=W0107


config_classes = [
    MailjetConfig,
    SESConfig
]


class EmailConfig(*config_classes): # pylint: disable=R0903
    """EmailConfig"""
    pass # pylint: disable=W0107
