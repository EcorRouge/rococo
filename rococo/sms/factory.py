from typing import Type, Dict, Tuple

from .enums import SMSProvider
from .base import SMSService
from .twilio import TwilioService
from .config import TwilioConfig, Config


class SMSServiceFactory:
    def __init__(self):
        self._services: Dict[str, Tuple[SMSService, Type[Config]]] = {}

    def register_service(self, key: SMSProvider, service: SMSService, config: Type[Config]):
        self._services[key] = (service, config)

    def _create(self, key: SMSProvider, **kwargs):
        service_class, config_class = self._services.get(key)

        if not service_class:
            raise ValueError(key)

        config = config_class(**kwargs)
        return service_class(config=config)

    def get(self, **kwargs):
        key = Config(**kwargs).SMS_PROVIDER
        return self._create(key, **kwargs)


sms_factory = SMSServiceFactory()

sms_factory.register_service(key=SMSProvider.TWILIO, service=TwilioService(), config=TwilioConfig)
