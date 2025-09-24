import re
from typing import Any, List, Union

from mailjet_rest import Client

from .base import EmailService
from .config import MailjetConfig


class MailjetService(EmailService):

    def __init__(self):
        pass

    @staticmethod
    def _convert_addresses(addresses: List[Union[str, dict]]) -> List[dict]:
        """
        Convert a list of addresses to Mailjet format.

        Args:
            addresses: List of email addresses as strings or dicts
                      - String format: "email@example.com"
                      - Dict format: {"Email": "email@example.com", "Name": "John Doe"}

        Returns:
            List of dicts in Mailjet format: [{"Email": "email@example.com", "Name": "John Doe"}]
        """
        converted_addresses = []

        for address in addresses:
            if isinstance(address, str):
                # Convert string to dict format
                converted_addresses.append({"Email": address})
            elif isinstance(address, dict):
                # Already in dict format, validate it has Email key
                if "Email" in address:
                    converted_addresses.append(address)
                else:
                    raise ValueError(
                        f"Dict address must contain 'Email' key: {address}")
            else:
                raise TypeError(
                    f"Address must be string or dict, got {type(address)}: {address}")

        return converted_addresses

    def __call__(self, config: MailjetConfig, *args, **kwargs):
        super().__call__(config)

        match = re.match(r'^(.*)\s*<(.*)>$', self.config.SOURCE_EMAIL)
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
        cc_addresses = message.get('cc_emails') or []
        bcc_addresses = message.get('bcc_emails') or []

        event_mapping = self.config.get_event(event_name)
        data = {
            'Messages': [
                {
                    "From": self.from_address,
                    "To": self._convert_addresses(to_addresses),
                    "Cc": self._convert_addresses(cc_addresses),
                    "Bcc": self._convert_addresses(bcc_addresses),
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

    def create_contact(self, email: str, first_name: str, last_name: str):
        raise NotImplementedError

    def remove_contact(self, email: str):
        raise NotImplementedError
