from enum import Enum


class SMSProvider(str, Enum):
    TWILIO = 'twilio'

    def __str__(self):
        return str(self.value)
