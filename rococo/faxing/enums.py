from enum import Enum


class FaxProvider(str, Enum):
    ifax = 'ifax'

    def __str__(self):
        return str(self.value)
