"""
Tests for email enums.

This module tests the EmailProvider enum that defines supported email providers.
"""
import unittest
from enum import Enum

from rococo.emailing.enums import EmailProvider


class TestEmailProviderEnum(unittest.TestCase):
    """Test EmailProvider enum."""

    def test_email_provider_is_enum(self):
        """
        Test that EmailProvider is an Enum.

        Verifies inheritance from Enum.
        """
        self.assertTrue(issubclass(EmailProvider, Enum))

    def test_email_provider_is_str_enum(self):
        """
        Test that EmailProvider inherits from str.

        Verifies it's a string enum for easy serialization.
        """
        self.assertTrue(issubclass(EmailProvider, str))

    def test_email_provider_has_mailjet(self):
        """
        Test that EmailProvider has mailjet member.

        Verifies mailjet provider is defined.
        """
        self.assertTrue(hasattr(EmailProvider, 'mailjet'))
        self.assertEqual(EmailProvider.mailjet.value, 'mailjet')

    def test_email_provider_has_ses(self):
        """
        Test that EmailProvider has ses member.

        Verifies SES provider is defined.
        """
        self.assertTrue(hasattr(EmailProvider, 'ses'))
        self.assertEqual(EmailProvider.ses.value, 'ses')

    def test_email_provider_mailjet_value(self):
        """
        Test that mailjet provider has correct value.

        Verifies value is lowercase 'mailjet'.
        """
        self.assertEqual(EmailProvider.mailjet, 'mailjet')
        self.assertEqual(str(EmailProvider.mailjet), 'mailjet')

    def test_email_provider_ses_value(self):
        """
        Test that ses provider has correct value.

        Verifies value is lowercase 'ses'.
        """
        self.assertEqual(EmailProvider.ses, 'ses')
        self.assertEqual(str(EmailProvider.ses), 'ses')

    def test_email_provider_str_method(self):
        """
        Test that __str__ returns the value.

        Verifies custom __str__ implementation.
        """
        self.assertEqual(str(EmailProvider.mailjet), 'mailjet')
        self.assertEqual(str(EmailProvider.ses), 'ses')

    def test_email_provider_comparison_with_string(self):
        """
        Test that EmailProvider can be compared with strings.

        Verifies string enum comparison works.
        """
        self.assertEqual(EmailProvider.mailjet, 'mailjet')
        self.assertEqual(EmailProvider.ses, 'ses')
        self.assertNotEqual(EmailProvider.mailjet, 'ses')

    def test_email_provider_can_be_created_from_string(self):
        """
        Test that EmailProvider can be created from string value.

        Verifies enum lookup.
        """
        provider1 = EmailProvider('mailjet')
        provider2 = EmailProvider('ses')
        self.assertEqual(provider1, EmailProvider.mailjet)
        self.assertEqual(provider2, EmailProvider.ses)

    def test_email_provider_invalid_value_raises_error(self):
        """
        Test that invalid provider value raises ValueError.

        Verifies enum validation.
        """
        with self.assertRaises(ValueError):
            EmailProvider('invalid_provider')

    def test_email_provider_members_count(self):
        """
        Test that EmailProvider has expected number of members.

        Verifies only defined providers exist.
        """
        members = list(EmailProvider)
        self.assertEqual(len(members), 2)
        self.assertIn(EmailProvider.mailjet, members)
        self.assertIn(EmailProvider.ses, members)


if __name__ == '__main__':
    unittest.main()
