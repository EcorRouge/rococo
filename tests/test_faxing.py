import unittest
from unittest.mock import patch

from rococo.faxing.factory import fax_service_factory
from rococo.faxing.enums import FaxProvider
from rococo.faxing.ifax import IFaxService


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
