import re
from typing import Any

from mailjet_rest import Client

from .base import EmailService
from .config import MailjetConfig


class MailjetService(EmailService):

    def __init__(self):
        pass

    def __call__(self, config: MailjetConfig, *args, **kwargs):
        super().__call__(config)

        match = re.match('^(.*)\s*<(.*)>$', self.config.SOURCE_EMAIL)
        name, email = match.groups()
        self.from_address = {"Name": name, "Email": email}

        self.client = Client(
            auth=(self.config.MAILJET_API_KEY, self.config.MAILJET_API_SECRET),
            version=self.config.MAILJET_API_VERSION
        )

        return self

    def send_email(self, message: dict) -> Any:
        event_name = message.get('event')
        event_data = message.get('data')
        to_addresses = message.get('to_emails')

        event_mapping = self.config.get_event(event_name)
        data = {
            'Messages': [
                {
                    "From": self.from_address,
                    "To": [{'Email': email} for email in to_addresses],
                    "TemplateLanguage": True,
                    "TemplateID": event_mapping['id'][self.config.EMAIL_PROVIDER],
                    "Variables": event_data
                }
            ]
        }
        if self.config.ERROR_REPORTING_EMAIL:
            data['Messages'][0]['TemplateErrorReporting'] = {
                'Email': self.config.ERROR_REPORTING_EMAIL
            }

        result = self.client.send.create(data=data)

        return result
