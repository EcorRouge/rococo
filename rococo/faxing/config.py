import json
import os.path
from pprint import pprint
from typing import Any, Dict

from pydantic.v1 import BaseSettings, Extra

from .enums import FaxProvider


class Config(BaseSettings):
    CONFIG_FILEPATH: str
    FAX_PROVIDER: FaxProvider

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
                if configuration['provider'] == self.FAX_PROVIDER:
                    self.provider_config = configuration
                    pprint(self.provider_config)
        
    def get_event(self, event_name: str) -> Dict:
        event = self.events.get(event_name)
        if event is None and event_name != 'DEFAULT':
            raise OSError(f'Service is improperly configured or event name "{event_name}" is invalid')
        return event or {}

    @property
    def FAX_SOURCE_NAME(self):
        return self.provider_config.get('sourceName')
    
    @property
    def FAX_SOURCE_NUMBER(self):
        return self.provider_config.get('sourceNumber')
    
    
class IFaxConfig(Config):
    IFAX_API_KEY: str

