from typing import Type, Dict, Tuple

from .enums import FaxProvider
from .base import FaxService
from .ifax import IFaxService
from .config import IFaxConfig, Config


class FaxServiceFactory:
    def __init__(self):
        self._services: Dict[str, Tuple[FaxService, Type[Config]]] = {}

    def register_service(self, key: FaxProvider, service: FaxService, config: Type[Config]):
        self._services[key] = (service, config)

    def _create(self, key: FaxProvider, **kwargs):
        service_class, config_class = self._services.get(key)

        if not service_class:
            raise ValueError(key)
        
        config = config_class(**kwargs)
        return service_class(config=config)

    def get(self, **kwargs) -> FaxService:
        key = Config(**kwargs).FAX_PROVIDER
        return self._create(key, **kwargs)


fax_service_factory = FaxServiceFactory()
fax_service_factory.register_service(key=FaxProvider.ifax, service=IFaxService(), config=IFaxConfig)
