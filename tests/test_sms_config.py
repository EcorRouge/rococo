"""
Tests for SMS configuration classes.

This module tests the Config and TwilioConfig classes that handle
SMS service configuration loading and validation.
"""
import unittest
import json
import tempfile
import os
from unittest.mock import Mock, patch, mock_open

from pydantic.v1 import ValidationError

from rococo.sms.config import Config, TwilioConfig, SMSConfig
from rococo.sms.enums import SMSProvider


# Test configuration data
TEST_CONFIG_VALID = {
    "events": {
        "welcome": {
            "type": "sms",
            "template": "Welcome {{name}}!",
            "default_parameters": {"name": "User"}
        },
        "verification": {
            "type": "sms",
            "template": "Your code is {{code}}",
            "default_parameters": {}
        }
    },
    "configurations": [
        {
            "provider": "twilio",
            "senderPhoneNumber": "+1234567890"
        }
    ]
}

TEST_CONFIG_WITH_MESSAGING_SERVICE = {
    "events": {
        "test": {
            "type": "sms",
            "template": "Test"
        }
    },
    "configurations": [
        {
            "provider": "twilio",
            "messagingServiceSid": "MGtest123"
        }
    ]
}

TEST_CONFIG_WITH_BOTH = {
    "events": {},
    "configurations": [
        {
            "provider": "twilio",
            "senderPhoneNumber": "+1234567890",
            "messagingServiceSid": "MGtest123"
        }
    ]
}

TEST_CONFIG_MISSING_SENDER = {
    "events": {},
    "configurations": [
        {
            "provider": "twilio"
            # Missing both senderPhoneNumber and messagingServiceSid
        }
    ]
}


class TestConfigClass(unittest.TestCase):
    """Test Config base class."""

    def test_config_initialization_reads_file(self):
        """
        Test that Config initialization reads and parses JSON file.

        Verifies:
        - JSON file is loaded
        - events are extracted
        - provider_config is found
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )

            self.assertEqual(config.CONFIG_FILEPATH, config_path)
            self.assertEqual(config.SMS_PROVIDER, SMSProvider.TWILIO)
            self.assertIsNotNone(config.events)
            self.assertIn('welcome', config.events)
            self.assertIsNotNone(config.provider_config)
        finally:
            os.unlink(config_path)

    def test_config_raises_error_for_missing_file(self):
        """
        Test that Config raises OSError if file doesn't exist.

        Verifies file validation.
        """
        with self.assertRaises(OSError) as context:
            Config(
                CONFIG_FILEPATH='/nonexistent/path/config.json',
                SMS_PROVIDER=SMSProvider.TWILIO
            )

        self.assertIn('Config.json file not found', str(context.exception))

    def test_config_get_event_returns_event_mapping(self):
        """
        Test that get_event returns the correct event configuration.

        Verifies event lookup functionality.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )

            event = config.get_event('welcome')
            self.assertEqual(event['type'], 'sms')
            self.assertEqual(event['template'], 'Welcome {{name}}!')
            self.assertEqual(event['default_parameters'], {'name': 'User'})
        finally:
            os.unlink(config_path)

    def test_config_get_event_returns_none_for_missing_event(self):
        """
        Test that get_event returns None for non-existent event.

        Verifies graceful handling of missing events.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )

            event = config.get_event('nonexistent')
            self.assertIsNone(event)
        finally:
            os.unlink(config_path)

    def test_config_sender_phone_number_property(self):
        """
        Test SENDER_PHONE_NUMBER property.

        Verifies property returns value from provider_config.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )

            self.assertEqual(config.SENDER_PHONE_NUMBER, '+1234567890')
        finally:
            os.unlink(config_path)

    def test_config_messaging_service_sid_property(self):
        """
        Test MESSAGING_SERVICE_SID property.

        Verifies property returns value from provider_config.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_WITH_MESSAGING_SERVICE, f)
            config_path = f.name

        try:
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )

            self.assertEqual(config.MESSAGING_SERVICE_SID, 'MGtest123')
        finally:
            os.unlink(config_path)

    def test_config_raises_error_for_twilio_without_sender_or_sid(self):
        """
        Test that Config raises ValueError for Twilio without sender or SID.

        Verifies Twilio configuration validation.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_MISSING_SENDER, f)
            config_path = f.name

        try:
            with self.assertRaises(ValueError) as context:
                Config(
                    CONFIG_FILEPATH=config_path,
                    SMS_PROVIDER=SMSProvider.TWILIO
                )

            self.assertIn('Missing required fields for Twilio', str(context.exception))
            self.assertIn('senderPhoneNumber', str(context.exception))
            self.assertIn('messagingServiceSid', str(context.exception))
        finally:
            os.unlink(config_path)

    def test_config_accepts_twilio_with_only_sender(self):
        """
        Test that Config accepts Twilio config with only senderPhoneNumber.

        Verifies validation allows either field.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            # Should not raise
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )
            self.assertIsNotNone(config.SENDER_PHONE_NUMBER)
        finally:
            os.unlink(config_path)

    def test_config_accepts_twilio_with_only_messaging_service_sid(self):
        """
        Test that Config accepts Twilio config with only messagingServiceSid.

        Verifies validation allows either field.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_WITH_MESSAGING_SERVICE, f)
            config_path = f.name

        try:
            # Should not raise
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )
            self.assertIsNotNone(config.MESSAGING_SERVICE_SID)
        finally:
            os.unlink(config_path)

    def test_config_accepts_twilio_with_both_fields(self):
        """
        Test that Config accepts Twilio config with both fields.

        Verifies validation allows both fields together.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_WITH_BOTH, f)
            config_path = f.name

        try:
            # Should not raise
            config = Config(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO
            )
            self.assertIsNotNone(config.SENDER_PHONE_NUMBER)
            self.assertIsNotNone(config.MESSAGING_SERVICE_SID)
        finally:
            os.unlink(config_path)


class TestTwilioConfig(unittest.TestCase):
    """Test TwilioConfig class."""

    def test_twilio_config_requires_account_sid(self):
        """
        Test that TwilioConfig requires TWILIO_ACCOUNT_SID.

        Verifies required field validation.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            with self.assertRaises(ValidationError) as context:
                TwilioConfig(
                    CONFIG_FILEPATH=config_path,
                    SMS_PROVIDER=SMSProvider.TWILIO,
                    # Missing TWILIO_ACCOUNT_SID
                    TWILIO_AUTH_TOKEN='token123'
                )

            self.assertIn('TWILIO_ACCOUNT_SID', str(context.exception))
        finally:
            os.unlink(config_path)

    def test_twilio_config_requires_auth_token(self):
        """
        Test that TwilioConfig requires TWILIO_AUTH_TOKEN.

        Verifies required field validation.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            with self.assertRaises(ValidationError) as context:
                TwilioConfig(
                    CONFIG_FILEPATH=config_path,
                    SMS_PROVIDER=SMSProvider.TWILIO,
                    TWILIO_ACCOUNT_SID='ACtest123',
                    # Missing TWILIO_AUTH_TOKEN
                )

            self.assertIn('TWILIO_AUTH_TOKEN', str(context.exception))
        finally:
            os.unlink(config_path)

    def test_twilio_config_initialization_with_all_fields(self):
        """
        Test that TwilioConfig initializes with all required fields.

        Verifies successful initialization.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = TwilioConfig(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO,
                TWILIO_ACCOUNT_SID='ACtest123',
                TWILIO_AUTH_TOKEN='token123'
            )

            self.assertEqual(config.TWILIO_ACCOUNT_SID, 'ACtest123')
            self.assertEqual(config.TWILIO_AUTH_TOKEN, 'token123')
            self.assertIsNotNone(config.events)
            self.assertIsNotNone(config.provider_config)
        finally:
            os.unlink(config_path)

    def test_twilio_config_inherits_from_config(self):
        """
        Test that TwilioConfig inherits from Config.

        Verifies inheritance hierarchy.
        """
        self.assertTrue(issubclass(TwilioConfig, Config))


class TestSMSConfig(unittest.TestCase):
    """Test SMSConfig union class."""

    def test_sms_config_includes_twilio_config(self):
        """
        Test that SMSConfig includes TwilioConfig in its base classes.

        Verifies union of all config classes.
        """
        # SMSConfig inherits from all config_classes
        self.assertTrue(issubclass(SMSConfig, TwilioConfig))

    def test_sms_config_can_be_instantiated_as_twilio(self):
        """
        Test that SMSConfig can be instantiated as TwilioConfig.

        Verifies union works correctly.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(TEST_CONFIG_VALID, f)
            config_path = f.name

        try:
            config = SMSConfig(
                CONFIG_FILEPATH=config_path,
                SMS_PROVIDER=SMSProvider.TWILIO,
                TWILIO_ACCOUNT_SID='ACtest123',
                TWILIO_AUTH_TOKEN='token123'
            )

            self.assertIsInstance(config, SMSConfig)
            self.assertIsInstance(config, TwilioConfig)
            self.assertEqual(config.TWILIO_ACCOUNT_SID, 'ACtest123')
        finally:
            os.unlink(config_path)


if __name__ == '__main__':
    unittest.main()
