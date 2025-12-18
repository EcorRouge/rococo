"""
Tests for email service factory.

This module tests the EmailServiceFactory class that creates email service
instances based on provider configuration.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock

from rococo.emailing.factory import EmailServiceFactory, email_factory
from rococo.emailing.enums import EmailProvider
from rococo.emailing.base import EmailService
from rococo.emailing.mailjet import MailjetService
from rococo.emailing.ses import SESService
from rococo.emailing.config import Config, MailjetConfig, SESConfig


class TestEmailServiceFactory(unittest.TestCase):
    """Test EmailServiceFactory class."""

    def test_factory_initialization(self):
        """
        Test that EmailServiceFactory initializes with empty services dict.

        Verifies initial state.
        """
        factory = EmailServiceFactory()
        self.assertIsInstance(factory, EmailServiceFactory)
        self.assertEqual(factory._services, {})

    def test_register_service(self):
        """
        Test that register_service stores service and config class.

        Verifies:
        - Service and config are stored as tuple
        - Key is EmailProvider enum
        """
        factory = EmailServiceFactory()
        mock_service = Mock(spec=EmailService)
        mock_config_class = Mock

        factory.register_service(
            key=EmailProvider.mailjet,
            service=mock_service,
            config=mock_config_class
        )

        self.assertIn(EmailProvider.mailjet, factory._services)
        service, config = factory._services[EmailProvider.mailjet]
        self.assertEqual(service, mock_service)
        self.assertEqual(config, mock_config_class)

    def test_register_multiple_services(self):
        """
        Test that multiple services can be registered.

        Verifies factory can handle multiple providers.
        """
        factory = EmailServiceFactory()
        mock_service1 = Mock(spec=EmailService)
        mock_service2 = Mock(spec=EmailService)
        mock_config_class = Mock

        factory.register_service(EmailProvider.mailjet, mock_service1, mock_config_class)
        factory.register_service(EmailProvider.ses, mock_service2, mock_config_class)

        # Verify both are registered
        self.assertEqual(len(factory._services), 2)
        self.assertIn(EmailProvider.mailjet, factory._services)
        self.assertIn(EmailProvider.ses, factory._services)

    @patch('rococo.emailing.factory.Config')
    def test_create_with_valid_key(self, mock_config_class):
        """
        Test that _create instantiates service with config.

        Verifies:
        - Config is instantiated with kwargs
        - Service is called with config
        - Returns configured service
        """
        factory = EmailServiceFactory()

        # Create mock service that implements __call__
        mock_service_instance = MagicMock()
        mock_configured_service = Mock()
        mock_service_instance.return_value = mock_configured_service

        mock_config_instance = Mock()
        mock_config_type = Mock(return_value=mock_config_instance)

        factory.register_service(
            key=EmailProvider.mailjet,
            service=mock_service_instance,
            config=mock_config_type
        )

        # Act
        kwargs = {'CONFIG_FILEPATH': '/path/to/config.json', 'MAILJET_API_KEY': 'key123'}
        result = factory._create(EmailProvider.mailjet, **kwargs)

        # Assert
        mock_config_type.assert_called_once_with(**kwargs)
        mock_service_instance.assert_called_once_with(config=mock_config_instance)
        self.assertEqual(result, mock_configured_service)

    def test_create_with_invalid_key_raises_error(self):
        """
        Test that _create raises ValueError for unregistered provider.

        Verifies error handling for unknown providers.
        """
        factory = EmailServiceFactory()

        # Attempt to create service without registering
        with self.assertRaises((ValueError, TypeError)):
            factory._create(EmailProvider.mailjet)

    @patch('rococo.emailing.factory.Config')
    def test_get_creates_service_from_config(self, mock_config_class):
        """
        Test that get method creates Config and calls _create.

        Verifies:
        - Config is instantiated with kwargs
        - EMAIL_PROVIDER is extracted from config
        - _create is called with provider key
        """
        factory = EmailServiceFactory()

        # Mock config instance
        mock_config_instance = Mock()
        mock_config_instance.EMAIL_PROVIDER = EmailProvider.mailjet
        mock_config_class.return_value = mock_config_instance

        # Mock service
        mock_service_instance = MagicMock()
        mock_configured_service = Mock()
        mock_service_instance.return_value = mock_configured_service

        mock_mailjet_config_type = Mock()
        mock_mailjet_config_instance = Mock()
        mock_mailjet_config_type.return_value = mock_mailjet_config_instance

        factory.register_service(
            key=EmailProvider.mailjet,
            service=mock_service_instance,
            config=mock_mailjet_config_type
        )

        # Act
        kwargs = {
            'CONFIG_FILEPATH': '/path/to/config.json',
            'EMAIL_PROVIDER': 'mailjet',
            'MAILJET_API_KEY': 'key123',
            'MAILJET_API_SECRET': 'secret123'
        }
        result = factory.get(**kwargs)

        # Assert
        mock_config_class.assert_called_once_with(**kwargs)
        mock_mailjet_config_type.assert_called_once_with(**kwargs)
        self.assertEqual(result, mock_configured_service)


class TestEmailFactorySingleton(unittest.TestCase):
    """Test the global email_factory singleton."""

    def test_email_factory_exists(self):
        """
        Test that email_factory singleton exists.

        Verifies global factory instance is available.
        """
        self.assertIsInstance(email_factory, EmailServiceFactory)

    def test_email_factory_has_mailjet_registered(self):
        """
        Test that Mailjet service is registered in the factory.

        Verifies:
        - mailjet key is registered
        - Service is MailjetService instance
        - Config is MailjetConfig class
        """
        self.assertIn(EmailProvider.mailjet, email_factory._services)

        service, config_class = email_factory._services[EmailProvider.mailjet]
        self.assertIsInstance(service, MailjetService)
        self.assertEqual(config_class, MailjetConfig)

    def test_email_factory_has_ses_registered(self):
        """
        Test that SES service is registered in the factory.

        Verifies:
        - ses key is registered
        - Service is SESService instance
        - Config is SESConfig class
        """
        self.assertIn(EmailProvider.ses, email_factory._services)

        service, config_class = email_factory._services[EmailProvider.ses]
        self.assertIsInstance(service, SESService)
        self.assertEqual(config_class, SESConfig)

    def test_email_factory_has_both_providers(self):
        """
        Test that factory has both email providers registered.

        Verifies complete provider registration.
        """
        self.assertEqual(len(email_factory._services), 2)
        self.assertIn(EmailProvider.mailjet, email_factory._services)
        self.assertIn(EmailProvider.ses, email_factory._services)


if __name__ == '__main__':
    unittest.main()
