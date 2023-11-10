import json
from typing import Dict

from pydantic import BaseSettings


class Config(BaseSettings):
    SERVICE_NAME: str
    SOURCE_EMAIL: str
    ERROR_REPORTING_EMAIL: str
    CONFIG_JSON_PATH: str

    _config: Dict

    def read_config_json(self):
        with open(self.CONFIG_JSON_PATH) as config:
            self._config = json.load(config)

    def get_config(self):
        for configuration in self._config['configurations']:
            if configuration['provider'] == self.SERVICE_NAME:
                return configuration

    def get_event(self, event_name: str):
        return self._config['events'][event_name]


class MailjetConfig(Config):
    SERVICE_NAME: str = 'mailjet'
    MAILJET_API_KEY: str
    MAILJET_API_SECRET: str
    MAILJET_API_VERSION: str = 'v3.1'


class SESConfig(Config):
    pass


config_classes = [MailjetConfig, SESConfig]


class EmailConfig(*config_classes):
    pass
