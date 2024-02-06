"""Email provider enum"""
from enum import Enum


class EmailProvider(str, Enum):
    """Email provider enum"""
    mailjet = 'mailjet'
    ses = 'ses'

    def __str__(self):
        return str(self.value)
