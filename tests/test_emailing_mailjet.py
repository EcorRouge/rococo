"""
Tests for Mailjet email service.

This module tests the MailjetService class methods for sending emails
and managing contacts via the Mailjet API.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
import re

from rococo.emailing.mailjet import MailjetService
from rococo.emailing.config import MailjetConfig
from rococo.emailing.enums import EmailProvider


# Test constants
TEST_API_KEY = "test_api_key_12345"
TEST_API_SECRET = "test_api_secret_67890"
TEST_API_VERSION = "v3.1"
TEST_SOURCE_EMAIL = "John Sender <sender@example.com>"
TEST_ERROR_REPORTING_EMAIL = "errors@example.com"
TEST_EVENT_NAME = "welcome_email"
TEST_TEMPLATE_ID = 12345
TEST_CONTACT_EMAIL = "contact@example.com"
TEST_CONTACT_NAME = "Test Contact"
TEST_LIST_ID = "list_123"
TEST_CONTACT_ID = 999


class TestMailjetServiceInit(unittest.TestCase):
    """Test MailjetService initialization."""

    def test_init_creates_instance(self):
        """
        Test that MailjetService can be instantiated.

        Verifies the constructor works without errors.
        """
        service = MailjetService()
        self.assertIsInstance(service, MailjetService)


class TestMailjetServiceCall(unittest.TestCase):
    """Test MailjetService __call__ method."""

    @patch('rococo.emailing.mailjet.Client')
    def test_call_parses_source_email_with_name(self, mock_client_class):
        """
        Test that __call__ parses SOURCE_EMAIL with regex to extract name and email.

        Verifies:
        - Regex parsing of "Name <email>" format
        - from_address dictionary is created
        - Client is initialized with API credentials
        """
        # Arrange
        mock_config = Mock(spec=MailjetConfig)
        mock_config.SOURCE_EMAIL = TEST_SOURCE_EMAIL
        mock_config.MAILJET_API_KEY = TEST_API_KEY
        mock_config.MAILJET_API_SECRET = TEST_API_SECRET
        mock_config.MAILJET_API_VERSION = TEST_API_VERSION
        mock_config.ERROR_REPORTING_EMAIL = None

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        service = MailjetService()

        # Act
        result = service(mock_config)

        # Assert
        # Note: Regex captures name with trailing space
        self.assertEqual(service.from_address, {
            "Name": "John Sender ",
            "Email": "sender@example.com"
        })
        mock_client_class.assert_called_once_with(
            auth=(TEST_API_KEY, TEST_API_SECRET),
            version=TEST_API_VERSION
        )
        self.assertEqual(service.client, mock_client)
        self.assertEqual(result, service)  # Returns self

    @patch('rococo.emailing.mailjet.Client')
    def test_call_stores_config(self, mock_client_class):
        """
        Test that __call__ stores the config instance.

        Verifies config is accessible after initialization.
        """
        # Arrange
        mock_config = Mock(spec=MailjetConfig)
        mock_config.SOURCE_EMAIL = TEST_SOURCE_EMAIL
        mock_config.MAILJET_API_KEY = TEST_API_KEY
        mock_config.MAILJET_API_SECRET = TEST_API_SECRET
        mock_config.MAILJET_API_VERSION = TEST_API_VERSION

        service = MailjetService()

        # Act
        service(mock_config)

        # Assert
        self.assertEqual(service.config, mock_config)


class TestMailjetSendEmail(unittest.TestCase):
    """Test MailjetService send_email method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=MailjetConfig)
        self.mock_config.SOURCE_EMAIL = TEST_SOURCE_EMAIL
        self.mock_config.MAILJET_API_KEY = TEST_API_KEY
        self.mock_config.MAILJET_API_SECRET = TEST_API_SECRET
        self.mock_config.MAILJET_API_VERSION = TEST_API_VERSION
        self.mock_config.ERROR_REPORTING_EMAIL = None
        self.mock_config.EMAIL_PROVIDER = EmailProvider.mailjet

        event_mapping = {
            'id': {
                EmailProvider.mailjet: TEST_TEMPLATE_ID
            }
        }
        self.mock_config.get_event.return_value = event_mapping

        self.mock_send_result = Mock()
        self.mock_send_result.json.return_value = {"Status": "success"}

        self.mock_client = MagicMock()
        self.mock_client.send.create.return_value = self.mock_send_result

    @patch('rococo.emailing.mailjet.Client')
    def test_send_email_with_to_addresses_only(self, mock_client_class):
        """
        Test send_email with only 'to' addresses.

        Verifies:
        - Message structure is correct
        - Template ID is looked up from event mapping
        - Client.send.create is called with proper data
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        service = MailjetService()
        service(self.mock_config)

        message = {
            'event': TEST_EVENT_NAME,
            'data': {'username': 'Alice', 'code': '123456'},
            'to_emails': ['user1@example.com', 'user2@example.com']
        }

        # Act
        result = service.send_email(message)

        # Assert
        self.mock_config.get_event.assert_called_once_with(TEST_EVENT_NAME)

        expected_data = {
            'Messages': [
                {
                    "From": {"Name": "John Sender ", "Email": "sender@example.com"},
                    "To": [{"Email": "user1@example.com"}, {"Email": "user2@example.com"}],
                    "Cc": [],
                    "Bcc": [],
                    "TemplateLanguage": True,
                    "TemplateID": TEST_TEMPLATE_ID,
                    "Variables": {'username': 'Alice', 'code': '123456'}
                }
            ]
        }
        self.mock_client.send.create.assert_called_once_with(data=expected_data)
        self.assertEqual(result, self.mock_send_result)

    @patch('rococo.emailing.mailjet.Client')
    def test_send_email_with_cc_and_bcc(self, mock_client_class):
        """
        Test send_email with cc and bcc addresses.

        Verifies:
        - CC and BCC addresses are included
        - Address conversion is applied to all address lists
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        service = MailjetService()
        service(self.mock_config)

        message = {
            'event': TEST_EVENT_NAME,
            'data': {},
            'to_emails': ['to@example.com'],
            'cc_emails': ['cc@example.com'],
            'bcc_emails': ['bcc@example.com']
        }

        # Act
        service.send_email(message)

        # Assert
        call_args = self.mock_client.send.create.call_args[1]['data']
        self.assertEqual(call_args['Messages'][0]['To'], [{"Email": "to@example.com"}])
        self.assertEqual(call_args['Messages'][0]['Cc'], [{"Email": "cc@example.com"}])
        self.assertEqual(call_args['Messages'][0]['Bcc'], [{"Email": "bcc@example.com"}])

    @patch('rococo.emailing.mailjet.Client')
    def test_send_email_with_error_reporting(self, mock_client_class):
        """
        Test send_email includes TemplateErrorReporting when configured.

        Verifies:
        - ERROR_REPORTING_EMAIL is included in message data
        - TemplateErrorReporting structure is correct
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        self.mock_config.ERROR_REPORTING_EMAIL = TEST_ERROR_REPORTING_EMAIL

        service = MailjetService()
        service(self.mock_config)

        message = {
            'event': TEST_EVENT_NAME,
            'data': {},
            'to_emails': ['to@example.com']
        }

        # Act
        service.send_email(message)

        # Assert
        call_args = self.mock_client.send.create.call_args[1]['data']
        self.assertIn('TemplateErrorReporting', call_args['Messages'][0])
        self.assertEqual(
            call_args['Messages'][0]['TemplateErrorReporting'],
            {'Email': TEST_ERROR_REPORTING_EMAIL}
        )

    @patch('rococo.emailing.mailjet.Client')
    def test_send_email_without_error_reporting(self, mock_client_class):
        """
        Test send_email excludes TemplateErrorReporting when not configured.

        Verifies conditional inclusion of error reporting.
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        self.mock_config.ERROR_REPORTING_EMAIL = None

        service = MailjetService()
        service(self.mock_config)

        message = {
            'event': TEST_EVENT_NAME,
            'data': {},
            'to_emails': ['to@example.com']
        }

        # Act
        service.send_email(message)

        # Assert
        call_args = self.mock_client.send.create.call_args[1]['data']
        self.assertNotIn('TemplateErrorReporting', call_args['Messages'][0])

    @patch('rococo.emailing.mailjet.Client')
    def test_send_email_handles_empty_cc_bcc(self, mock_client_class):
        """
        Test send_email handles None/missing cc_emails and bcc_emails.

        Verifies:
        - None values are converted to empty lists
        - Missing keys default to empty lists
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        service = MailjetService()
        service(self.mock_config)

        # Message without cc_emails and bcc_emails keys
        message = {
            'event': TEST_EVENT_NAME,
            'data': {},
            'to_emails': ['to@example.com']
        }

        # Act
        service.send_email(message)

        # Assert
        call_args = self.mock_client.send.create.call_args[1]['data']
        self.assertEqual(call_args['Messages'][0]['Cc'], [])
        self.assertEqual(call_args['Messages'][0]['Bcc'], [])


class TestMailjetCreateContact(unittest.TestCase):
    """Test MailjetService create_contact method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=MailjetConfig)
        self.mock_config.SOURCE_EMAIL = TEST_SOURCE_EMAIL
        self.mock_config.MAILJET_API_KEY = TEST_API_KEY
        self.mock_config.MAILJET_API_SECRET = TEST_API_SECRET
        self.mock_config.MAILJET_API_VERSION = TEST_API_VERSION

        self.mock_client = MagicMock()

    @patch('rococo.emailing.mailjet.Client')
    def test_create_contact_success(self, mock_client_class):
        """
        Test create_contact successfully creates a new contact.

        Verifies:
        - Contact data structure is correct
        - Contact ID is extracted from response
        - Extra custom data is updated if provided
        - Contact is added to list if list_id provided
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        # Mock successful contact creation
        mock_create_response = Mock()
        mock_create_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.create.return_value = mock_create_response

        service = MailjetService()
        service(self.mock_config)

        extra_data = {'custom_field1': 'value1', 'custom_field2': 'value2'}

        # Act
        service.create_contact(
            email=TEST_CONTACT_EMAIL,
            name=TEST_CONTACT_NAME,
            list_id=TEST_LIST_ID,
            extra=extra_data
        )

        # Assert
        expected_contact_data = {
            "IsExcludedFromCampaigns": "true",
            "Name": TEST_CONTACT_NAME,
            "Email": TEST_CONTACT_EMAIL
        }
        self.mock_client.contact.create.assert_called_once_with(data=expected_contact_data)

        # Verify custom data update
        expected_custom_data = {
            "Data": [
                {"Name": "custom_field1", "Value": "value1"},
                {"Name": "custom_field2", "Value": "value2"}
            ]
        }
        self.mock_client.contactdata.update.assert_called_once_with(
            id=TEST_CONTACT_ID,
            data=expected_custom_data
        )

        # Verify list membership
        expected_list_data = {
            "IsUnsubscribed": "true",
            "ContactID": TEST_CONTACT_ID,
            "ListID": TEST_LIST_ID
        }
        self.mock_client.listrecipient.create.assert_called_once_with(data=expected_list_data)

    @patch('rococo.emailing.mailjet.Client')
    def test_create_contact_handles_duplicate(self, mock_client_class):
        """
        Test create_contact handles duplicate contact (ErrorMessage in response).

        Verifies:
        - Duplicate error is caught
        - Contact is searched by email
        - Existing contact ID is retrieved
        - Updates proceed with existing contact
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        # Mock duplicate contact creation (contains ErrorMessage)
        mock_create_response = Mock()
        mock_create_response.json.return_value = {
            "ErrorMessage": "Object already exists"
        }
        self.mock_client.contact.create.return_value = mock_create_response

        # Mock contact search
        mock_get_response = Mock()
        mock_get_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.get.return_value = mock_get_response

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.create_contact(
            email=TEST_CONTACT_EMAIL,
            name=TEST_CONTACT_NAME,
            list_id=TEST_LIST_ID,
            extra={'field': 'value'}
        )

        # Assert
        # Should attempt to create
        self.mock_client.contact.create.assert_called_once()

        # Should search for existing contact
        self.mock_client.contact.get.assert_called_once_with(id=TEST_CONTACT_EMAIL)

        # Should proceed with updates using existing contact ID
        self.mock_client.contactdata.update.assert_called_once_with(
            id=TEST_CONTACT_ID,
            data={"Data": [{"Name": "field", "Value": "value"}]}
        )
        self.mock_client.listrecipient.create.assert_called_once()

    @patch('rococo.emailing.mailjet.Client')
    def test_create_contact_without_extra_data(self, mock_client_class):
        """
        Test create_contact skips custom data update when extra is empty.

        Verifies:
        - contactdata.update is NOT called when extra is empty
        - Other operations still proceed normally
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        mock_create_response = Mock()
        mock_create_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.create.return_value = mock_create_response

        service = MailjetService()
        service(self.mock_config)

        # Act - pass empty dict for extra
        service.create_contact(
            email=TEST_CONTACT_EMAIL,
            name=TEST_CONTACT_NAME,
            list_id=TEST_LIST_ID,
            extra={}
        )

        # Assert
        self.mock_client.contact.create.assert_called_once()
        self.mock_client.contactdata.update.assert_not_called()
        self.mock_client.listrecipient.create.assert_called_once()

    @patch('rococo.emailing.mailjet.Client')
    def test_create_contact_without_list_id(self, mock_client_class):
        """
        Test create_contact skips list assignment when list_id is empty.

        Verifies:
        - listrecipient.create is NOT called when list_id is empty
        - Other operations still proceed normally
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        mock_create_response = Mock()
        mock_create_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.create.return_value = mock_create_response

        service = MailjetService()
        service(self.mock_config)

        # Act - pass empty string for list_id
        service.create_contact(
            email=TEST_CONTACT_EMAIL,
            name=TEST_CONTACT_NAME,
            list_id="",
            extra={'field': 'value'}
        )

        # Assert
        self.mock_client.contact.create.assert_called_once()
        self.mock_client.contactdata.update.assert_called_once()
        self.mock_client.listrecipient.create.assert_not_called()

    @patch('rococo.emailing.mailjet.Client')
    def test_create_contact_with_no_extra_and_no_list(self, mock_client_class):
        """
        Test create_contact with minimal parameters (no extra, no list).

        Verifies:
        - Contact is still created
        - No updates or list assignments occur
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        mock_create_response = Mock()
        mock_create_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.create.return_value = mock_create_response

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.create_contact(
            email=TEST_CONTACT_EMAIL,
            name=TEST_CONTACT_NAME,
            list_id="",
            extra={}
        )

        # Assert
        self.mock_client.contact.create.assert_called_once()
        self.mock_client.contactdata.update.assert_not_called()
        self.mock_client.listrecipient.create.assert_not_called()


class TestMailjetRemoveContact(unittest.TestCase):
    """Test MailjetService remove_contact method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = Mock(spec=MailjetConfig)
        self.mock_config.SOURCE_EMAIL = TEST_SOURCE_EMAIL
        self.mock_config.MAILJET_API_KEY = TEST_API_KEY
        self.mock_config.MAILJET_API_SECRET = TEST_API_SECRET
        self.mock_config.MAILJET_API_VERSION = TEST_API_VERSION

        self.mock_client = MagicMock()

    @patch('rococo.emailing.mailjet.requests.delete')
    @patch('rococo.emailing.mailjet.Client')
    def test_remove_contact_success(self, mock_client_class, mock_requests_delete):
        """
        Test remove_contact successfully deletes a contact.

        Verifies:
        - Contact is searched by email
        - Contact ID is extracted
        - DELETE request is made to correct URL
        - Auth credentials are passed
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        # Mock contact search
        mock_get_response = Mock()
        mock_get_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.get.return_value = mock_get_response

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.remove_contact(TEST_CONTACT_EMAIL)

        # Assert
        self.mock_client.contact.get.assert_called_once_with(id=TEST_CONTACT_EMAIL)

        expected_url = f"https://api.mailjet.com/v4/contacts/{TEST_CONTACT_ID}"
        mock_requests_delete.assert_called_once_with(
            expected_url,
            auth=(TEST_API_KEY, TEST_API_SECRET),
            timeout=15
        )

    @patch('rococo.emailing.mailjet.requests.delete')
    @patch('rococo.emailing.mailjet.Client')
    @patch('rococo.emailing.mailjet.logger')
    def test_remove_contact_not_found(self, mock_logger, mock_client_class, mock_requests_delete):
        """
        Test remove_contact handles contact not found gracefully.

        Verifies:
        - Exception during contact.get is caught
        - Warning is logged
        - Function returns early without attempting delete
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        # Mock contact search raises exception
        self.mock_client.contact.get.side_effect = Exception("Contact not found")

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.remove_contact(TEST_CONTACT_EMAIL)

        # Assert
        self.mock_client.contact.get.assert_called_once_with(id=TEST_CONTACT_EMAIL)
        mock_logger.exception.assert_called_once()
        log_message = mock_logger.exception.call_args[0][0]
        self.assertIn("Couldn't find Mailjet contact", log_message)
        self.assertIn(TEST_CONTACT_EMAIL, log_message)

        # Should not attempt delete
        mock_requests_delete.assert_not_called()

    @patch('rococo.emailing.mailjet.requests.delete')
    @patch('rococo.emailing.mailjet.Client')
    @patch('rococo.emailing.mailjet.logger')
    def test_remove_contact_delete_fails(self, mock_logger, mock_client_class, mock_requests_delete):
        """
        Test remove_contact handles delete failure gracefully.

        Verifies:
        - Exception during requests.delete is caught
        - Warning is logged
        - Function continues (doesn't crash)
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        mock_get_response = Mock()
        mock_get_response.json.return_value = {
            "Data": [{"ID": TEST_CONTACT_ID}]
        }
        self.mock_client.contact.get.return_value = mock_get_response

        # Mock delete raises exception
        mock_requests_delete.side_effect = Exception("Delete failed")

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.remove_contact(TEST_CONTACT_EMAIL)

        # Assert
        self.mock_client.contact.get.assert_called_once()
        mock_requests_delete.assert_called_once()

        # Should log exception
        mock_logger.exception.assert_called_once()
        log_message = mock_logger.exception.call_args[0][0]
        self.assertIn("Couldn't remove Mailjet contact", log_message)
        self.assertIn(TEST_CONTACT_EMAIL, log_message)

    @patch('rococo.emailing.mailjet.requests.delete')
    @patch('rococo.emailing.mailjet.Client')
    def test_remove_contact_multiple_contacts(self, mock_client_class, mock_requests_delete):
        """
        Test remove_contact handles multiple contacts with same email.

        Verifies:
        - All matching contacts are deleted
        - DELETE is called once for each contact ID
        """
        # Arrange
        mock_client_class.return_value = self.mock_client

        # Mock multiple contacts returned
        mock_get_response = Mock()
        mock_get_response.json.return_value = {
            "Data": [
                {"ID": 111},
                {"ID": 222},
                {"ID": 333}
            ]
        }
        self.mock_client.contact.get.return_value = mock_get_response

        service = MailjetService()
        service(self.mock_config)

        # Act
        service.remove_contact(TEST_CONTACT_EMAIL)

        # Assert
        self.assertEqual(mock_requests_delete.call_count, 3)

        expected_calls = [
            call("https://api.mailjet.com/v4/contacts/111",
                 auth=(TEST_API_KEY, TEST_API_SECRET), timeout=15),
            call("https://api.mailjet.com/v4/contacts/222",
                 auth=(TEST_API_KEY, TEST_API_SECRET), timeout=15),
            call("https://api.mailjet.com/v4/contacts/333",
                 auth=(TEST_API_KEY, TEST_API_SECRET), timeout=15)
        ]
        mock_requests_delete.assert_has_calls(expected_calls)


if __name__ == '__main__':
    unittest.main()
