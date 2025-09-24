import re
import requests
import logging
from typing import Any, List, Union

from mailjet_rest import Client

from .base import EmailService
from .config import MailjetConfig


logger = logging.getLogger(__name__)


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

    def create_contact(self, email: str, first_name: str, last_name: str, extra: dict):
        contact_data = {
            "IsExcludedFromCampaigns": "true",
            "Name": f"{first_name} {last_name}",
            "Email": email,
        }

        resp = self.client.contact.create(data=contact_data)
        if "ErrorMessage" in resp.json():
            # Search for the contact
            result = self.client.contact.get(id=email)
            contact_id = result.json()["Data"][0]["ID"]
        else:
            contact_id = resp.json()["Data"][0]["ID"]

        contact_data = {
            "Data": [
                {"Name": "first_name", "Value": first_name},
                {"Name": "last_name", "Value": last_name},
                {"Name": "Organization", "Value": extra["organization_name"]},
                {"Name": "Organization_ID", "Value": extra["organization_id"]},
                {"Name": "Organization_Type",
                    "Value": extra["organization_type"]},
            ]
        }

        # Update contact with data
        self.client.contactdata.update(id=contact_id, data=contact_data)

        # Add contact to list
        data = {
            "IsUnsubscribed": "true",
            "ContactID": contact_id,
            "ListID": "56160",
        }
        self.client.listrecipient.create(data=data)

    def remove_contact(self, email: str):
        # Find the contact to get the ID
        try:
            result = self.client.contact.get(id=email)
            response_data = result.json()["Data"]
        except Exception as e:
            message = f"Couldn't find Mailjet contact for {email}: {e}"
            logger.exception(message, "warning")
            return

        for contact in response_data:
            contact_id = contact["ID"]
            url = f"https://api.mailjet.com/v4/contacts/{contact_id}"
            try:
                requests.delete(url, auth=(
                    self.config.MAILJET_API_KEY, self.config.MAILJET_API_SECRET), timeout=15)
            except Exception as e:
                message = f"Couldn't remove Mailjet contact for {email} : {e}"
                logger.exception(message, "warning")
