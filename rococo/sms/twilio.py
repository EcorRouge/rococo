import re
import requests
import logging
from typing import Any, List, Union

from twilio.rest import Client
from jinja2 import Template

from .base import SMSService
from .config import TwilioConfig


logger = logging.getLogger(__name__)


class TwilioService(SMSService):

    def __init__(self):
        pass


    def __call__(self, config: TwilioConfig, *args, **kwargs):
        super().__call__(config)

        self.client = Client(
            self.config.TWILIO_ACCOUNT_SID,
            self.config.TWILIO_AUTH_TOKEN
        )

        return self

    def send_sms(self, event_name: str, phone_number: str, parameters: dict) -> Any:
        event_mapping = self.config.get_event(event_name)
        event_type = event_mapping.get('type')
        default_parameters = event_mapping.get('default_parameters', {})
        event_parameters = {**default_parameters, **parameters}
        event_template = event_mapping.get('template')

        # Render the jinja2 template with event parameters
        template = Template(event_template)
        message_body = template.render(**event_parameters)

        if event_type.lower() == 'sms':
            # Send an SMS from Twilio client
            params = {}
            if self.config.MESSAGING_SERVICE_SID:
                params['messaging_service_sid'] = self.config.MESSAGING_SERVICE_SID
            if self.config.SENDER_PHONE_NUMBER:
                params['from_'] = self.config.SENDER_PHONE_NUMBER
            message = self.client.messages.create(
                body=message_body,
                to=phone_number,
                **params
            )
            logger.info(f"SMS sent successfully. SID: {message.sid}")
            return message
        else:
            raise ValueError(f"Unsupported event type: {event_type}")