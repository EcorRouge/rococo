import unittest
from unittest.mock import patch, mock_open, MagicMock, Mock
import base64

from rococo.faxing.factory import fax_service_factory
from rococo.faxing.enums import FaxProvider
from rococo.faxing.ifax import IFaxService
from rococo.faxing.config import IFaxConfig


class TestFaxing(unittest.TestCase):
    def setUp(self):
        self.config_path = 'tests/fixtures/config.json'

    def test_service_factory_returns_correct_service(self):
        service = fax_service_factory.get(
            CONFIG_FILEPATH=self.config_path,
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )
        self.assertIsInstance(service, IFaxService)

    def test_config_is_read_correctly(self):
        service = fax_service_factory.get(
            CONFIG_FILEPATH=self.config_path,
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )
        self.assertEqual(service.config.FAX_SOURCE_NAME, "Test Fax")
        self.assertEqual(service.config.FAX_SOURCE_NUMBER, "1234567890")

    def test_prepare_send_fax_request_data(self):
        service = fax_service_factory.get(
            CONFIG_FILEPATH=self.config_path,
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )
        message = {
            'recipient': {'number': '0987654321', 'name': 'Test Recipient'},
            'message': 'Test message',
            'subject': 'Test subject',
            'faxes': []
        }
        with patch.object(service, '_prepare_fax_data', return_value=[]) as mock_prepare_fax_data:
            request_data = service._prepare_send_fax_request_data(message, {})
            self.assertEqual(request_data['faxNumber'], '0987654321')
            self.assertEqual(request_data['to_name'], 'Test Recipient')
            self.assertEqual(request_data['message'], 'Test message')
            self.assertEqual(request_data['subject'], 'Test subject')
            mock_prepare_fax_data.assert_called_once_with([])

    def test_wait_for_fax_result(self):
        service = fax_service_factory.get(
            CONFIG_FILEPATH=self.config_path,
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )
        with patch.object(service, 'get_fax_status') as mock_get_fax_status:
            mock_get_fax_status.side_effect = [
                {'faxStatus': 'sending', 'message': ''},
                {'faxStatus': 'success', 'message': 'Fax sent successfully'}
            ]
            status, message = service.wait_for_fax_result('test_job_id')
            self.assertEqual(status, 'success')
            self.assertEqual(message, 'Fax sent successfully')
            self.assertEqual(mock_get_fax_status.call_count, 2)


class TestIFaxServiceCall(unittest.TestCase):
    """Test IFaxService __call__ method."""

    def test_call_sets_source_name_and_number(self):
        """Test that __call__ initializes source name and number from config."""
        service = IFaxService()
        config = IFaxConfig(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key',
            FAX_SOURCE_NAME='Test Source',
            FAX_SOURCE_NUMBER='1234567890'
        )

        result = service(config)

        self.assertIs(result, service)
        # Values come from config file, not from parameters
        self.assertEqual(service.source_name, config.FAX_SOURCE_NAME)
        self.assertEqual(service.source_number, config.FAX_SOURCE_NUMBER)
        self.assertEqual(service.config, config)


class TestIFaxServiceSendFax(unittest.TestCase):
    """Test IFaxService send_fax method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    @patch('rococo.faxing.config.Config.get_event')
    @patch.object(IFaxService, 'wait_for_fax_result')
    @patch.object(IFaxService, 'send_fax_request')
    @patch.object(IFaxService, '_prepare_send_fax_request_data')
    def test_send_fax_success(self, mock_prepare, mock_send_request, mock_wait, mock_get_event):
        """Test send_fax with successful result."""
        mock_get_event.return_value = {}
        mock_prepare.return_value = {'test': 'data'}
        mock_send_request.return_value = 'job_123'
        mock_wait.return_value = ('success', 'Fax sent successfully')

        message = {'event': 'test_event', 'recipient': {'number': '123'}}
        self.service.send_fax(message)

        mock_get_event.assert_called_once_with('test_event')
        mock_send_request.assert_called_once_with({'test': 'data'})
        mock_wait.assert_called_once_with('job_123')

    @patch('rococo.faxing.config.Config.get_event')
    @patch.object(IFaxService, 'wait_for_fax_result')
    @patch.object(IFaxService, 'send_fax_request')
    @patch.object(IFaxService, '_prepare_send_fax_request_data')
    def test_send_fax_retry_on_failure(self, mock_prepare, mock_send_request, mock_wait, mock_get_event):
        """Test send_fax retries on failure."""
        mock_get_event.return_value = {}
        mock_prepare.return_value = {'test': 'data'}
        mock_send_request.side_effect = ['job_1', 'job_2']
        mock_wait.side_effect = [
            ('failed', 'Error message'),
            ('success', 'Fax sent successfully')
        ]

        message = {'event': 'test_event', 'recipient': {'number': '123'}}
        self.service.send_fax(message)

        self.assertEqual(mock_send_request.call_count, 2)
        self.assertEqual(mock_wait.call_count, 2)

    @patch('rococo.faxing.config.Config.get_event')
    @patch.object(IFaxService, 'wait_for_fax_result')
    @patch.object(IFaxService, 'send_fax_request')
    @patch.object(IFaxService, '_prepare_send_fax_request_data')
    def test_send_fax_raises_after_max_retries(self, mock_prepare, mock_send_request, mock_wait, mock_get_event):
        """Test send_fax raises ValueError after exhausting retries."""
        mock_get_event.return_value = {}
        mock_prepare.return_value = {'test': 'data'}
        mock_send_request.return_value = 'job_123'
        mock_wait.return_value = ('failed', 'Persistent error')

        message = {'event': 'test_event', 'recipient': {'number': '123'}}

        with self.assertRaises(ValueError) as context:
            self.service.send_fax(message, retry_count=0)

        self.assertIn('Sending fax failed', str(context.exception))
        self.assertIn('Persistent error', str(context.exception))


class TestIFaxServiceSendFaxRequest(unittest.TestCase):
    """Test IFaxService send_fax_request method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    @patch.object(IFaxService, '_send_request')
    def test_send_fax_request_returns_job_id(self, mock_send):
        """Test send_fax_request returns job ID from response."""
        mock_send.return_value = {'jobId': 'job_abc123'}

        request_data = {'test': 'data'}
        job_id = self.service.send_fax_request(request_data)

        self.assertEqual(job_id, 'job_abc123')
        mock_send.assert_called_once_with(self.service.send_fax_url, request_data)


class TestIFaxServiceGetFaxStatus(unittest.TestCase):
    """Test IFaxService get_fax_status method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    @patch.object(IFaxService, '_send_request')
    def test_get_fax_status(self, mock_send):
        """Test get_fax_status returns status data."""
        mock_send.return_value = {'faxStatus': 'success', 'message': 'Sent'}

        result = self.service.get_fax_status('job_123')

        self.assertEqual(result, {'faxStatus': 'success', 'message': 'Sent'})
        mock_send.assert_called_once_with(
            self.service.get_fax_status_url,
            {'jobId': 'job_123'}
        )


class TestIFaxServicePrepareSendFaxRequestData(unittest.TestCase):
    """Test IFaxService _prepare_send_fax_request_data method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    def test_raises_error_without_recipient_number(self):
        """Test raises ValueError when recipient.number is missing."""
        message = {'recipient': {'name': 'Test'}}
        with self.assertRaises(ValueError) as context:
            self.service._prepare_send_fax_request_data(message, {})

        self.assertIn('recipient.number', str(context.exception))
        self.assertIn('required', str(context.exception))

    @patch.object(IFaxService, '_prepare_fax_data')
    def test_uses_event_data_defaults(self, mock_prepare_fax):
        """Test uses event_data defaults when message values missing."""
        mock_prepare_fax.return_value = []

        message = {
            'recipient': {'number': '123', 'name': 'Test'},
            'faxes': []
        }
        event_data = {
            'message': 'Default message',
            'subject': 'Default subject',
            'fax_quality': 'high'
        }

        result = self.service._prepare_send_fax_request_data(message, event_data)

        self.assertEqual(result['message'], 'Default message')
        self.assertEqual(result['subject'], 'Default subject')
        self.assertEqual(result['faxQuality'], 'high')

    @patch.object(IFaxService, '_prepare_fax_data')
    def test_message_overrides_event_data(self, mock_prepare_fax):
        """Test message values override event_data defaults."""
        mock_prepare_fax.return_value = []

        message = {
            'recipient': {'number': '123'},
            'message': 'Custom message',
            'subject': 'Custom subject',
            'fax_quality': 'standard',
            'send_at': '2025-01-01',
            'faxes': []
        }
        event_data = {'message': 'Default', 'subject': 'Default'}

        result = self.service._prepare_send_fax_request_data(message, event_data)

        self.assertEqual(result['message'], 'Custom message')
        self.assertEqual(result['subject'], 'Custom subject')
        self.assertEqual(result['faxQuality'], 'standard')
        self.assertEqual(result['send_at'], '2025-01-01')


class TestIFaxServicePrepareFaxData(unittest.TestCase):
    """Test IFaxService _prepare_fax_data method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    @patch.object(IFaxService, '_prepare_s3_file_data')
    def test_handles_s3_files(self, mock_s3):
        """Test processes S3 files correctly."""
        mock_s3.return_value = {'fileName': 'test.pdf', 'fileUrl': 'https://s3.url'}

        fax_files = [
            {'filename': 'test.pdf', 'type': 's3', 'path': 's3://bucket/file.pdf'}
        ]

        result = self.service._prepare_fax_data(fax_files)

        self.assertEqual(len(result), 1)
        mock_s3.assert_called_once_with('test.pdf', fax_files[0])

    @patch.object(IFaxService, '_prepare_local_file_data')
    def test_handles_local_files(self, mock_local):
        """Test processes local files correctly."""
        mock_local.return_value = {'fileName': 'doc.pdf', 'fileData': 'base64data'}

        fax_files = [
            {'filename': 'doc.pdf', 'type': 'local', 'path': '/path/to/file.pdf'}
        ]

        result = self.service._prepare_fax_data(fax_files)

        self.assertEqual(len(result), 1)
        mock_local.assert_called_once_with('doc.pdf', fax_files[0])

    def test_raises_error_for_invalid_type(self):
        """Test raises ValueError for invalid file type."""
        fax_files = [
            {'type': 'invalid_type', 'path': '/some/path'}
        ]

        with self.assertRaises(ValueError) as context:
            self.service._prepare_fax_data(fax_files)

        self.assertIn('Invalid value', str(context.exception))
        self.assertIn('invalid_type', str(context.exception))

    @patch.object(IFaxService, '_prepare_local_file_data')
    def test_uses_default_filename(self, mock_local):
        """Test uses default filename when not provided."""
        mock_local.return_value = {'fileName': 'document_1.pdf', 'fileData': 'data'}

        fax_files = [
            {'type': 'local', 'path': '/path/to/file.pdf'}  # No filename
        ]

        self.service._prepare_fax_data(fax_files)

        # Should be called with default filename
        mock_local.assert_called_once_with('document_1.pdf', fax_files[0])


class TestIFaxServicePrepareS3FileData(unittest.TestCase):
    """Test IFaxService _prepare_s3_file_data method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    def test_raises_error_for_missing_aws_key(self):
        """Test raises ValueError when aws_key is missing."""
        fax_file = {
            'aws_secret_key': 'secret',
            'aws_region': 'us-east-1',
            'path': 's3://bucket/file.pdf'
        }

        with self.assertRaises(ValueError) as context:
            self.service._prepare_s3_file_data('test.pdf', fax_file)

        self.assertIn('aws_key', str(context.exception))

    def test_raises_error_for_invalid_s3_path(self):
        """Test raises ValueError for invalid S3 path format."""
        fax_file = {
            'aws_key': 'key',
            'aws_secret_key': 'secret',
            'aws_region': 'us-east-1',
            'path': '/local/path/file.pdf'  # Not s3://
        }

        with self.assertRaises(ValueError) as context:
            self.service._prepare_s3_file_data('test.pdf', fax_file)

        self.assertIn('valid S3 path', str(context.exception))

    def test_raises_error_for_malformed_s3_path(self):
        """Test raises ValueError for S3 path without bucket."""
        fax_file = {
            'aws_key': 'key',
            'aws_secret_key': 'secret',
            'aws_region': 'us-east-1',
            'path': 's3://bucketonly'  # No object key
        }

        with self.assertRaises(ValueError) as context:
            self.service._prepare_s3_file_data('test.pdf', fax_file)

        self.assertIn('valid S3 path', str(context.exception))

    @patch('rococo.faxing.ifax.boto3.client')
    def test_generates_presigned_url(self, mock_boto3_client):
        """Test generates presigned URL for S3 file."""
        mock_s3_client = Mock()
        mock_s3_client.generate_presigned_url.return_value = 'https://presigned.url'
        mock_boto3_client.return_value = mock_s3_client

        fax_file = {
            'aws_key': 'test_key',
            'aws_secret_key': 'test_secret',
            'aws_region': 'us-west-2',
            'path': 's3://my-bucket/path/to/file.pdf'
        }

        result = self.service._prepare_s3_file_data('test.pdf', fax_file)

        # Verify boto3 client created with credentials
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-west-2'
        )

        # Verify presigned URL generated
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            'get_object',
            Params={'Bucket': 'my-bucket', 'Key': 'path/to/file.pdf'},
            ExpiresIn=3600
        )

        # Verify result format
        self.assertEqual(result['fileName'], 'test.pdf')
        self.assertEqual(result['fileUrl'], 'https://presigned.url')


class TestIFaxServicePrepareLocalFileData(unittest.TestCase):
    """Test IFaxService _prepare_local_file_data method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    def test_raises_error_for_missing_path(self):
        """Test raises ValueError when path is missing."""
        fax_file = {'filename': 'test.pdf'}

        with self.assertRaises(ValueError) as context:
            self.service._prepare_local_file_data('test.pdf', fax_file)

        self.assertIn('path', str(context.exception))
        self.assertIn('required', str(context.exception))

    @patch('builtins.open', new_callable=mock_open, read_data=b'test file content')
    def test_encodes_file_to_base64(self, mock_file):
        """Test reads file and encodes to base64."""
        fax_file = {'path': '/path/to/test.pdf'}

        result = self.service._prepare_local_file_data('test.pdf', fax_file)

        # Verify file was opened in binary mode
        mock_file.assert_called_once_with('/path/to/test.pdf', 'rb')

        # Verify base64 encoding
        expected_base64 = base64.b64encode(b'test file content').decode('utf-8')
        self.assertEqual(result['fileName'], 'test.pdf')
        self.assertEqual(result['fileData'], expected_base64)


class TestIFaxServiceSendRequest(unittest.TestCase):
    """Test IFaxService _send_request method."""

    def setUp(self):
        self.service = fax_service_factory.get(
            CONFIG_FILEPATH='tests/fixtures/config.json',
            FAX_PROVIDER=FaxProvider.ifax,
            IFAX_API_KEY='test_key'
        )

    @patch('rococo.faxing.ifax.requests.post')
    def test_sends_request_with_auth_header(self, mock_post):
        """Test sends request with API key in headers."""
        mock_response = Mock()
        mock_response.json.return_value = {
            'status': 1,
            'data': {'jobId': 'job_123'}
        }
        mock_post.return_value = mock_response

        request_data = {'test': 'data'}
        result = self.service._send_request('https://api.url', request_data)

        # Verify request sent correctly
        mock_post.assert_called_once_with(
            'https://api.url',
            json=request_data,
            headers={'accessToken': 'test_key'}
        )

        # Verify result
        self.assertEqual(result, {'jobId': 'job_123'})

    @patch('rococo.faxing.ifax.requests.post')
    def test_raises_error_on_api_failure(self, mock_post):
        """Test raises RuntimeError when API returns status 0."""
        mock_response = Mock()
        mock_response.json.return_value = {
            'status': 0,
            'message': 'API error occurred'
        }
        mock_post.return_value = mock_response

        request_data = {'test': 'data'}

        with self.assertRaises(RuntimeError) as context:
            self.service._send_request('https://api.url', request_data)

        self.assertIn('API error occurred', str(context.exception))
