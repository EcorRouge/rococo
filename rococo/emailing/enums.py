from enum import Enum


class EmailService(str, Enum):
    mailjet = 'mailjet'
    ses = 'ses'

    def __str__(self):
        return str(self.value)
