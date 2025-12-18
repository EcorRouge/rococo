"""
Tests for SMS enums.

This module tests the SMSProvider enum that defines supported SMS providers.
"""
import unittest
from enum import Enum

from rococo.sms.enums import SMSProvider


class TestSMSProviderEnum(unittest.TestCase):
    """Test SMSProvider enum."""

    def test_sms_provider_is_enum(self):
        """
        Test that SMSProvider is an Enum.

        Verifies inheritance from Enum.
        """
        self.assertTrue(issubclass(SMSProvider, Enum))

    def test_sms_provider_is_str_enum(self):
        """
        Test that SMSProvider inherits from str.

        Verifies it's a string enum for easy serialization.
        """
        self.assertTrue(issubclass(SMSProvider, str))

    def test_sms_provider_has_twilio(self):
        """
        Test that SMSProvider has TWILIO member.

        Verifies TWILIO provider is defined.
        """
        self.assertTrue(hasattr(SMSProvider, 'TWILIO'))
        self.assertEqual(SMSProvider.TWILIO.value, 'twilio')

    def test_sms_provider_twilio_value(self):
        """
        Test that TWILIO provider has correct value.

        Verifies value is lowercase 'twilio'.
        """
        self.assertEqual(SMSProvider.TWILIO, 'twilio')
        self.assertEqual(str(SMSProvider.TWILIO), 'twilio')

    def test_sms_provider_str_method(self):
        """
        Test that __str__ returns the value.

        Verifies custom __str__ implementation.
        """
        provider = SMSProvider.TWILIO
        self.assertEqual(str(provider), 'twilio')

    def test_sms_provider_comparison_with_string(self):
        """
        Test that SMSProvider can be compared with strings.

        Verifies string enum comparison works.
        """
        self.assertEqual(SMSProvider.TWILIO, 'twilio')
        self.assertNotEqual(SMSProvider.TWILIO, 'other')

    def test_sms_provider_can_be_created_from_string(self):
        """
        Test that SMSProvider can be created from string value.

        Verifies enum lookup.
        """
        provider = SMSProvider('twilio')
        self.assertEqual(provider, SMSProvider.TWILIO)

    def test_sms_provider_invalid_value_raises_error(self):
        """
        Test that invalid provider value raises ValueError.

        Verifies enum validation.
        """
        with self.assertRaises(ValueError):
            SMSProvider('invalid_provider')

    def test_sms_provider_members_count(self):
        """
        Test that SMSProvider has expected number of members.

        Verifies only defined providers exist.
        """
        members = list(SMSProvider)
        self.assertEqual(len(members), 1)
        self.assertIn(SMSProvider.TWILIO, members)


if __name__ == '__main__':
    unittest.main()
