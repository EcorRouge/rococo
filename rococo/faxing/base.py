from abc import ABC, abstractmethod
from typing import Dict

from .config import Config

class FaxService(ABC):
    config: Config

    def __call__(self, config: Config, *args, **kwargs):
        self.config = config

    @abstractmethod
    def send_fax(self, message: Dict):
        raise NotImplementedError()
        