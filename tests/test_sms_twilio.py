"""
Tests for Twilio SMS service.

This module tests the TwilioService class which integrates with the Twilio API
to send SMS messages using event-based templates.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call

from rococo.sms.twilio import TwilioService
from rococo.sms.config import TwilioConfig


# Test constants
TEST_ACCOUNT_SID = "ACtest123456789"
TEST_AUTH_TOKEN = "test_auth_token_12345"
TEST_PHONE_NUMBER = "+1234567890"
TEST_SENDER_PHONE = "+0987654321"
TEST_MESSAGING_SERVICE_SID = "MGtest123456789"
TEST_MESSAGE_SID = "SMtest123456789"
TEST_EVENT_NAME = "test_event"
TEST_TEMPLATE = "Hello {{name}}, your code is {{code}}"
TEST_RENDERED_MESSAGE = "Hello Alice, your code is 123456"


class TestTwilioServiceInit(unittest.TestCase):
    """Test TwilioService initialization."""

    def test_init_creates_instance(self):
        """
        Test that TwilioService can be instantiated.

        Verifies the constructor works without errors.
        """
        service = TwilioService()
        self.assertIsInstance(service, TwilioService)


class TestTwilioServiceCall(unittest.TestCase):
    """Test TwilioService __call__ method."""

    @patch('rococo.sms.twilio.Client')
    def test_call_creates_twilio_client(self, mock_client_class):
        """
        Test that __call__ creates Twilio Client with credentials.

        Verifies:
        - Client is created with TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN
        - Config is stored in self.config
        - Returns self for chaining
        """
        # Arrange
        mock_config = Mock(spec=TwilioConfig)
        mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        service = TwilioService()

        # Act
        result = service(mock_config)

        # Assert
        mock_client_class.assert_called_once_with(TEST_ACCOUNT_SID, TEST_AUTH_TOKEN)
        self.assertEqual(service.config, mock_config)
        self.assertEqual(service.client, mock_client)
        self.assertEqual(result, service)  # Returns self

    @patch('rococo.sms.twilio.Client')
    def test_call_stores_config(self, mock_client_class):
        """
        Test that __call__ stores the config instance.

        Verifies config is accessible after initialization.
        """
        # Arrange
        mock_config = Mock(spec=TwilioConfig)
        mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        service = TwilioService()

        # Act
        service(mock_config)

        # Assert
        self.assertEqual(service.config, mock_config)


class TestTwilioSendSms(unittest.TestCase):
    """Test TwilioService send_sms method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=TwilioConfig)
        self.mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        self.mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        self.mock_config.SENDER_PHONE_NUMBER = TEST_SENDER_PHONE
        self.mock_config.MESSAGING_SERVICE_SID = None

        self.mock_message = Mock()
        self.mock_message.sid = TEST_MESSAGE_SID

        self.mock_client = MagicMock()
        self.mock_client.messages.create.return_value = self.mock_message

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_with_sender_phone_number(self, mock_client_class):
        """
        Test send_sms with SENDER_PHONE_NUMBER.

        Verifies:
        - Event mapping is retrieved
        - Template is rendered with parameters
        - SMS is sent with from_ parameter
        - Message SID is logged
        - Returns message object
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': TEST_TEMPLATE,
            'default_parameters': {'name': 'Alice'}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        result = service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={'code': '123456'}
        )

        # Assert
        self.mock_config.get_event.assert_called_once_with(TEST_EVENT_NAME)
        self.mock_client.messages.create.assert_called_once_with(
            body=TEST_RENDERED_MESSAGE,
            to=TEST_PHONE_NUMBER,
            from_=TEST_SENDER_PHONE
        )
        self.assertEqual(result, self.mock_message)

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_with_messaging_service_sid(self, mock_client_class):
        """
        Test send_sms with MESSAGING_SERVICE_SID instead of sender phone.

        Verifies:
        - messaging_service_sid parameter is used
        - from_ parameter is not included
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        self.mock_config.SENDER_PHONE_NUMBER = None
        self.mock_config.MESSAGING_SERVICE_SID = TEST_MESSAGING_SERVICE_SID

        event_mapping = {
            'type': 'sms',
            'template': 'Simple message',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        _result = service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={}
        )

        # Assert
        self.mock_client.messages.create.assert_called_once_with(
            body='Simple message',
            to=TEST_PHONE_NUMBER,
            messaging_service_sid=TEST_MESSAGING_SERVICE_SID
        )

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_with_both_sender_and_service_sid(self, mock_client_class):
        """
        Test send_sms when both SENDER_PHONE_NUMBER and MESSAGING_SERVICE_SID are set.

        Verifies:
        - Both parameters are included in the API call
        - Twilio API will handle the priority
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        self.mock_config.SENDER_PHONE_NUMBER = TEST_SENDER_PHONE
        self.mock_config.MESSAGING_SERVICE_SID = TEST_MESSAGING_SERVICE_SID

        event_mapping = {
            'type': 'sms',
            'template': 'Test message',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={}
        )

        # Assert
        self.mock_client.messages.create.assert_called_once_with(
            body='Test message',
            to=TEST_PHONE_NUMBER,
            messaging_service_sid=TEST_MESSAGING_SERVICE_SID,
            from_=TEST_SENDER_PHONE
        )

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_merges_default_and_provided_parameters(self, mock_client_class):
        """
        Test that send_sms merges default parameters with provided parameters.

        Verifies:
        - Default parameters from event mapping are used
        - Provided parameters override defaults
        - All parameters are available to template
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': '{{greeting}} {{name}}, your {{item}}: {{value}}',
            'default_parameters': {
                'greeting': 'Hello',
                'item': 'code'
            }
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={
                'name': 'Bob',
                'value': '999',
                'item': 'token'  # Override default
            }
        )

        # Assert
        # Provided 'item' should override default 'code' -> 'token'
        expected_body = 'Hello Bob, your token: 999'
        self.mock_client.messages.create.assert_called_once()
        call_kwargs = self.mock_client.messages.create.call_args[1]
        self.assertEqual(call_kwargs['body'], expected_body)

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_without_default_parameters(self, mock_client_class):
        """
        Test send_sms when event has no default_parameters.

        Verifies:
        - Missing default_parameters key is handled gracefully
        - Only provided parameters are used
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': 'Hello {{name}}'
            # No 'default_parameters' key
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={'name': 'Charlie'}
        )

        # Assert
        self.mock_client.messages.create.assert_called_once()
        call_kwargs = self.mock_client.messages.create.call_args[1]
        self.assertEqual(call_kwargs['body'], 'Hello Charlie')


class TestTwilioTemplateRendering(unittest.TestCase):
    """Test Jinja2 template rendering."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=TwilioConfig)
        self.mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        self.mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        self.mock_config.SENDER_PHONE_NUMBER = TEST_SENDER_PHONE
        self.mock_config.MESSAGING_SERVICE_SID = None

        self.mock_message = Mock()
        self.mock_message.sid = TEST_MESSAGE_SID

        self.mock_client = MagicMock()
        self.mock_client.messages.create.return_value = self.mock_message

    @patch('rococo.sms.twilio.Client')
    def test_template_rendering_with_complex_parameters(self, mock_client_class):
        """
        Test that Jinja2 template is correctly rendered with complex parameters.

        Verifies template engine handles various data types.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': 'Order {{order_id}}: {{items|length}} items, Total: ${{total}}',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={
                'order_id': '12345',
                'items': ['item1', 'item2', 'item3'],
                'total': 99.99
            }
        )

        # Assert
        call_kwargs = self.mock_client.messages.create.call_args[1]
        self.assertEqual(call_kwargs['body'], 'Order 12345: 3 items, Total: $99.99')

    @patch('rococo.sms.twilio.Client')
    def test_template_rendering_with_missing_parameter(self, mock_client_class):
        """
        Test that template rendering handles missing parameters gracefully.

        Verifies Jinja2 leaves undefined variables empty by default.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': 'Hello {{name}}, your code: {{code}}',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={'name': 'David'}  # Missing 'code'
        )

        # Assert
        call_kwargs = self.mock_client.messages.create.call_args[1]
        # Jinja2 leaves undefined variables empty by default
        self.assertEqual(call_kwargs['body'], 'Hello David, your code: ')


class TestTwilioEventMapping(unittest.TestCase):
    """Test event type mapping and validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=TwilioConfig)
        self.mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        self.mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        self.mock_config.SENDER_PHONE_NUMBER = TEST_SENDER_PHONE
        self.mock_config.MESSAGING_SERVICE_SID = None

        self.mock_message = Mock()
        self.mock_message.sid = TEST_MESSAGE_SID

        self.mock_client = MagicMock()
        self.mock_client.messages.create.return_value = self.mock_message

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_with_uppercase_event_type(self, mock_client_class):
        """
        Test that event type check is case-insensitive.

        Verifies 'SMS', 'Sms', 'sms' all work.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'SMS',  # Uppercase
            'template': 'Test message',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        result = service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={}
        )

        # Assert - Should not raise ValueError
        self.assertEqual(result, self.mock_message)

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_raises_error_for_unsupported_event_type(self, mock_client_class):
        """
        Test that unsupported event types raise ValueError.

        Verifies only 'sms' type is supported.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'email',  # Unsupported
            'template': 'Test message',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            service.send_sms(
                event_name=TEST_EVENT_NAME,
                phone_number=TEST_PHONE_NUMBER,
                parameters={}
            )

        self.assertIn('Unsupported event type', str(context.exception))
        self.assertIn('email', str(context.exception))

    @patch('rococo.sms.twilio.Client')
    def test_send_sms_raises_error_for_unknown_event_type(self, mock_client_class):
        """
        Test that unknown/invalid event types raise ValueError.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'push_notification',
            'template': 'Test',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            service.send_sms(
                event_name=TEST_EVENT_NAME,
                phone_number=TEST_PHONE_NUMBER,
                parameters={}
            )

        self.assertIn('Unsupported event type', str(context.exception))


class TestTwilioLogging(unittest.TestCase):
    """Test logging functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=TwilioConfig)
        self.mock_config.TWILIO_ACCOUNT_SID = TEST_ACCOUNT_SID
        self.mock_config.TWILIO_AUTH_TOKEN = TEST_AUTH_TOKEN
        self.mock_config.SENDER_PHONE_NUMBER = TEST_SENDER_PHONE
        self.mock_config.MESSAGING_SERVICE_SID = None

        self.mock_message = Mock()
        self.mock_message.sid = TEST_MESSAGE_SID

        self.mock_client = MagicMock()
        self.mock_client.messages.create.return_value = self.mock_message

    @patch('rococo.sms.twilio.logger')
    @patch('rococo.sms.twilio.Client')
    def test_send_sms_logs_success(self, mock_client_class, mock_logger):
        """
        Test that successful SMS sends are logged.

        Verifies:
        - Info log is created
        - Log includes message SID
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        event_mapping = {
            'type': 'sms',
            'template': 'Test',
            'default_parameters': {}
        }
        self.mock_config.get_event.return_value = event_mapping

        service = TwilioService()
        service(self.mock_config)

        # Act
        service.send_sms(
            event_name=TEST_EVENT_NAME,
            phone_number=TEST_PHONE_NUMBER,
            parameters={}
        )

        # Assert
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        self.assertIn('SMS sent successfully', log_message)
        self.assertIn(TEST_MESSAGE_SID, log_message)


if __name__ == '__main__':
    unittest.main()
