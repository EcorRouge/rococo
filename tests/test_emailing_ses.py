"""
Tests for AWS SES email service stub.

This module tests the SESService stub implementation.
"""
import unittest
from unittest.mock import Mock

from rococo.emailing.ses import SESService
from rococo.emailing.config import SESConfig


class TestSESServiceInit(unittest.TestCase):
    """Test SESService initialization."""

    def test_init_creates_instance(self):
        """
        Test that SESService can be instantiated.

        Verifies the constructor works without errors.
        """
        service = SESService()
        self.assertIsInstance(service, SESService)


class TestSESServiceCall(unittest.TestCase):
    """Test SESService __call__ method."""

    def test_call_stores_config(self):
        """
        Test that __call__ stores the config instance.

        Verifies:
        - Config is stored in self.config
        - Returns self for chaining
        """
        # Arrange
        mock_config = Mock(spec=SESConfig)
        service = SESService()

        # Act
        result = service(mock_config)

        # Assert
        self.assertEqual(service.config, mock_config)
        self.assertEqual(result, service)  # Returns self


class TestSESServiceStubMethods(unittest.TestCase):
    """Test SESService stub method implementations."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = SESService()
        self.mock_config = Mock(spec=SESConfig)
        self.service(self.mock_config)

    def test_send_email_stub(self):
        """
        Test that send_email is a stub (returns None).

        Verifies stub implementation doesn't raise errors.
        """
        message = {
            'event': 'test_event',
            'data': {},
            'to_emails': ['test@example.com']
        }

        # Should not raise, returns None
        result = self.service.send_email(message)
        self.assertIsNone(result)

    def test_create_contact_stub(self):
        """
        Test that create_contact is a stub (returns None).

        Verifies stub implementation doesn't raise errors.
        """
        # Should not raise, returns None
        result = self.service.create_contact(
            email='test@example.com',
            name='Test User',
            list_id='list123',
            extra={'field': 'value'}
        )
        self.assertIsNone(result)

    def test_remove_contact_stub(self):
        """
        Test that remove_contact is a stub (returns None).

        Verifies stub implementation doesn't raise errors.
        """
        # Should not raise, returns None
        result = self.service.remove_contact('test@example.com')
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
