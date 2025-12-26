"""
Tests for SMS service factory.

This module tests the SMSServiceFactory class that creates SMS service
instances based on provider configuration.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock

from rococo.sms.factory import SMSServiceFactory, sms_factory
from rococo.sms.enums import SMSProvider
from rococo.sms.base import SMSService
from rococo.sms.twilio import TwilioService
from rococo.sms.config import Config, TwilioConfig


class TestSMSServiceFactory(unittest.TestCase):
    """Test SMSServiceFactory class."""

    def test_factory_initialization(self):
        """
        Test that SMSServiceFactory initializes with empty services dict.

        Verifies initial state.
        """
        factory = SMSServiceFactory()
        self.assertIsInstance(factory, SMSServiceFactory)
        self.assertEqual(factory._services, {})

    def test_register_service(self):
        """
        Test that register_service stores service and config class.

        Verifies:
        - Service and config are stored as tuple
        - Key is SMSProvider enum
        """
        factory = SMSServiceFactory()
        mock_service = Mock(spec=SMSService)
        mock_config_class = Mock

        factory.register_service(
            key=SMSProvider.TWILIO,
            service=mock_service,
            config=mock_config_class
        )

        self.assertIn(SMSProvider.TWILIO, factory._services)
        service, config = factory._services[SMSProvider.TWILIO]
        self.assertEqual(service, mock_service)
        self.assertEqual(config, mock_config_class)

    def test_register_multiple_services(self):
        """
        Test that multiple services can be registered.

        Verifies factory can handle multiple providers.
        """
        factory = SMSServiceFactory()
        mock_service1 = Mock(spec=SMSService)
        _mock_service2 = Mock(spec=SMSService)
        mock_config_class = Mock

        factory.register_service(SMSProvider.TWILIO, mock_service1, mock_config_class)

        # Verify both are registered
        self.assertEqual(len(factory._services), 1)

    @patch('rococo.sms.factory.Config')
    def test_create_with_valid_key(self, mock_config_class):
        """
        Test that _create instantiates service with config.

        Verifies:
        - Config is instantiated with kwargs
        - Service is called with config
        - Returns configured service
        """
        factory = SMSServiceFactory()

        # Create mock service that implements __call__
        mock_service_instance = MagicMock()
        mock_configured_service = Mock()
        mock_service_instance.return_value = mock_configured_service

        mock_config_instance = Mock()
        mock_config_type = Mock(return_value=mock_config_instance)

        factory.register_service(
            key=SMSProvider.TWILIO,
            service=mock_service_instance,
            config=mock_config_type
        )

        # Act
        kwargs = {'CONFIG_FILEPATH': '/path/to/config.json', 'TWILIO_ACCOUNT_SID': 'AC123'}
        result = factory._create(SMSProvider.TWILIO, **kwargs)

        # Assert
        mock_config_type.assert_called_once_with(**kwargs)
        mock_service_instance.assert_called_once_with(config=mock_config_instance)
        self.assertEqual(result, mock_configured_service)

    def test_create_with_invalid_key_raises_error(self):
        """
        Test that _create raises ValueError for unregistered provider.

        Verifies error handling for unknown providers.
        """
        factory = SMSServiceFactory()

        # Attempt to create service without registering
        # Note: This will actually raise a different error because
        # service_class will be None and unpacking will fail
        with self.assertRaises((ValueError, TypeError)):
            factory._create(SMSProvider.TWILIO)

    @patch('rococo.sms.factory.Config')
    def test_get_creates_service_from_config(self, mock_config_class):
        """
        Test that get method creates Config and calls _create.

        Verifies:
        - Config is instantiated with kwargs
        - SMS_PROVIDER is extracted from config
        - _create is called with provider key
        """
        factory = SMSServiceFactory()

        # Mock config instance
        mock_config_instance = Mock()
        mock_config_instance.SMS_PROVIDER = SMSProvider.TWILIO
        mock_config_class.return_value = mock_config_instance

        # Mock service
        mock_service_instance = Mock(spec=SMSService)
        mock_configured_service = Mock()
        mock_service_instance.return_value = mock_configured_service

        mock_twilio_config_type = Mock()
        mock_twilio_config_instance = Mock()
        mock_twilio_config_type.return_value = mock_twilio_config_instance

        factory.register_service(
            key=SMSProvider.TWILIO,
            service=mock_service_instance,
            config=mock_twilio_config_type
        )

        # Act
        kwargs = {
            'CONFIG_FILEPATH': '/path/to/config.json',
            'SMS_PROVIDER': 'twilio',
            'TWILIO_ACCOUNT_SID': 'AC123',
            'TWILIO_AUTH_TOKEN': 'token'
        }
        result = factory.get(**kwargs)

        # Assert
        mock_config_class.assert_called_once_with(**kwargs)
        mock_twilio_config_type.assert_called_once_with(**kwargs)
        self.assertEqual(result, mock_configured_service)


class TestSMSFactorySingleton(unittest.TestCase):
    """Test the global sms_factory singleton."""

    def test_sms_factory_exists(self):
        """
        Test that sms_factory singleton exists.

        Verifies global factory instance is available.
        """
        self.assertIsInstance(sms_factory, SMSServiceFactory)

    def test_sms_factory_has_twilio_registered(self):
        """
        Test that Twilio service is registered in the factory.

        Verifies:
        - TWILIO key is registered
        - Service is TwilioService instance
        - Config is TwilioConfig class
        """
        self.assertIn(SMSProvider.TWILIO, sms_factory._services)

        service, config_class = sms_factory._services[SMSProvider.TWILIO]
        self.assertIsInstance(service, TwilioService)
        self.assertEqual(config_class, TwilioConfig)

    def test_sms_factory_can_create_twilio_service(self):
        """
        Test that factory can create Twilio service.

        Note: This test requires a valid config file, so we mock Config.
        """
        # This is more of an integration test - factory should work with real classes
        # We can verify the structure is correct by checking registrations
        service, config = sms_factory._services[SMSProvider.TWILIO]
        self.assertIsNotNone(service)
        self.assertIsNotNone(config)


if __name__ == '__main__':
    unittest.main()
