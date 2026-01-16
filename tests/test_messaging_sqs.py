"""
Tests for AWS SQS message queue connection and operations.

This module tests the SQS connection class which handles
message sending, consuming, and queue management.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import uuid

from rococo.messaging.sqs import SqsConnection


# Test constants
TEST_AWS_KEY_ID = "test_access_key_id"
TEST_AWS_KEY_SECRET = "test_access_key_secret"
TEST_REGION = "us-east-1"
TEST_QUEUE_NAME = "test_queue"
TEST_MESSAGE = {"key": "value", "id": 123}
TEST_CONFIG_PATH = "/path/to/config.env"


class TestSqsInit(unittest.TestCase):
    """Test SQS connection initialization."""

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_init_stores_connection_parameters(self, mock_boto3_resource):
        """
        Test that initialization stores all connection parameters.

        Verifies that AWS credentials and region are stored.
        """
        mock_sqs = MagicMock()
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        self.assertEqual(connection._aws_access_key_id, TEST_AWS_KEY_ID)
        self.assertEqual(connection._aws_access_key_secret, TEST_AWS_KEY_SECRET)
        self.assertEqual(connection._region_name, TEST_REGION)
        self.assertEqual(connection._consume_config_file_path, TEST_CONFIG_PATH)

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_init_creates_boto3_resource(self, mock_boto3_resource):
        """
        Test that initialization creates boto3 SQS resource.

        Verifies proper boto3 setup with credentials.
        """
        mock_sqs = MagicMock()
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        mock_boto3_resource.assert_called_once_with(
            'sqs',
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_secret_access_key=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )
        self.assertEqual(connection._sqs, mock_sqs)

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_init_initializes_empty_queue_map(self, mock_boto3_resource):
        """
        Test that initialization creates empty queue map for caching.

        Verifies queue caching mechanism initialization.
        """
        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        self.assertEqual(connection._queue_map, {})

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_init_without_optional_parameters(self, mock_boto3_resource):
        """
        Test initialization without optional parameters.

        Verifies defaults are handled correctly.
        """
        connection = SqsConnection()

        self.assertIsNone(connection._aws_access_key_id)
        self.assertIsNone(connection._aws_access_key_secret)
        self.assertIsNone(connection._region_name)
        self.assertIsNone(connection._consume_config_file_path)


class TestSqsContextManager(unittest.TestCase):
    """Test SQS context manager protocol (__enter__ and __exit__)."""

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_enter_returns_self(self, mock_boto3_resource):
        """
        Test that __enter__ returns self.

        Verifies context manager entry.
        """
        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        result = connection.__enter__()

        self.assertEqual(result, connection)

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_exit_does_nothing(self, mock_boto3_resource):
        """
        Test that __exit__ is a no-op for SQS.

        Verifies SQS doesn't require cleanup (connections managed by boto3).
        """
        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        # Should not raise any exception
        connection.__exit__(None, None, None)


class TestSqsReadConsumeConfig(unittest.TestCase):
    """Test consume configuration reading."""

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_read_consume_config_returns_empty_dict_if_no_path(self, mock_boto3_resource):
        """
        Test that _read_consume_config returns empty dict when no path provided.

        Verifies default behavior without config file.
        """
        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        result = connection._read_consume_config()

        self.assertEqual(result, {})

    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_read_consume_config_loads_from_file(self, mock_boto3_resource, mock_dotenv):
        """
        Test that _read_consume_config loads from file when path provided.

        Verifies config file loading.
        """
        mock_dotenv.return_value = {
            'EXIT_WHEN_FINISHED': '1',
            'LISTEN_INTERVAL': '30',
            'VISIBILITY_TIMEOUT': '60'
        }

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        result = connection._read_consume_config()

        self.assertEqual(result['EXIT_WHEN_FINISHED'], '1')
        self.assertEqual(result['LISTEN_INTERVAL'], '30')
        self.assertEqual(result['VISIBILITY_TIMEOUT'], '60')
        mock_dotenv.assert_called_once_with(TEST_CONFIG_PATH)


class TestSqsSendMessage(unittest.TestCase):
    """Test message sending functionality."""

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_send_message_creates_queue_on_first_send(self, mock_boto3_resource):
        """
        Test that sending message creates queue on first send.

        Verifies queue creation for new queues.
        """
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        connection.send_message(TEST_QUEUE_NAME, TEST_MESSAGE)

        mock_sqs.create_queue.assert_called_once_with(QueueName=TEST_QUEUE_NAME)
        mock_queue.send_message.assert_called_once_with(
            QueueUrl=TEST_QUEUE_NAME,
            MessageBody=json.dumps(TEST_MESSAGE)
        )

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_send_message_reuses_cached_queue(self, mock_boto3_resource):
        """
        Test that sending message reuses cached queue from queue_map.

        Verifies queue caching mechanism when queue is pre-cached.
        """
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        # Pre-cache the queue
        connection._queue_map[TEST_QUEUE_NAME] = mock_queue

        # Send message - should use cached queue
        connection.send_message(TEST_QUEUE_NAME, TEST_MESSAGE)

        # Should NOT create queue since it's cached
        mock_sqs.create_queue.assert_not_called()
        # Should send message once
        mock_queue.send_message.assert_called_once()

    @patch('rococo.messaging.sqs.boto3.resource')
    def test_send_message_json_serializes_dict(self, mock_boto3_resource):
        """
        Test that message dict is JSON serialized.

        Verifies proper message formatting.
        """
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION
        )

        message = {"test": "data", "number": 42}
        connection.send_message(TEST_QUEUE_NAME, message)

        expected_body = json.dumps(message)
        mock_queue.send_message.assert_called_once_with(
            QueueUrl=TEST_QUEUE_NAME,
            MessageBody=expected_body
        )


class TestSqsConsumeMessages(unittest.TestCase):
    """Test message consumption functionality."""

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_creates_queue(self, mock_boto3_resource, mock_dotenv, mock_uuid):
        """
        Test that consume_messages creates the queue.

        Verifies queue creation for consumption.
        """
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        # Return empty list to trigger exit
        mock_queue.receive_messages.return_value = []
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        mock_sqs.create_queue.assert_called_once_with(QueueName=TEST_QUEUE_NAME)

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_calls_receive_with_default_config(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages calls receive_messages with default config.

        Verifies default polling configuration.
        """
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_queue.receive_messages.return_value = []
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        mock_queue.receive_messages.assert_called_once_with(
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # Default LISTEN_INTERVAL
        )

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_uses_visibility_timeout_if_configured(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages uses VISIBILITY_TIMEOUT if configured.

        Verifies visibility timeout configuration.
        """
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_dotenv.return_value = {
            'EXIT_WHEN_FINISHED': '1',
            'VISIBILITY_TIMEOUT': '60',
            'LISTEN_INTERVAL': '30'
        }
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_queue.receive_messages.return_value = []
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        mock_queue.receive_messages.assert_called_once_with(
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=30,
            VisibilityTimeout=60
        )

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_processes_message_and_calls_callback(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages processes message and calls callback.

        Verifies message processing and callback invocation.
        """
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue

        # First call returns message, second call returns empty to exit
        mock_message = MagicMock()
        mock_message.body = json.dumps(TEST_MESSAGE)
        mock_message.receipt_handle = "test_receipt_handle"
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        callback.assert_called_once_with(TEST_MESSAGE)

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_deletes_message_after_processing(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages deletes message after processing.

        Verifies message deletion.
        """
        test_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_uuid.return_value = test_uuid
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue

        mock_message = MagicMock()
        mock_message.body = json.dumps(TEST_MESSAGE)
        mock_message.receipt_handle = "test_receipt_handle"
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        mock_queue.delete_messages.assert_called_once_with(
            Entries=[
                {
                    'Id': str(test_uuid),
                    'ReceiptHandle': "test_receipt_handle"
                }
            ]
        )

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_exits_when_no_messages_and_exit_flag_set(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages exits when no messages and EXIT_WHEN_FINISHED=1.

        Verifies exit condition for batch processing.
        """
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue
        mock_queue.receive_messages.return_value = []
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock()
        # Should return (exit) instead of continuing loop
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        # Callback should not be called since no messages
        callback.assert_not_called()

    @patch('rococo.messaging.sqs.uuid.uuid4')
    @patch('rococo.messaging.sqs.dotenv_values')
    @patch('rococo.messaging.sqs.boto3.resource')
    def test_consume_messages_handles_callback_exception(
        self, mock_boto3_resource, mock_dotenv, mock_uuid
    ):
        """
        Test that consume_messages handles exceptions in callback gracefully.

        Verifies error handling and that message still gets deleted.
        """
        test_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
        mock_uuid.return_value = test_uuid
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}
        mock_sqs = MagicMock()
        mock_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_queue

        mock_message = MagicMock()
        mock_message.body = json.dumps(TEST_MESSAGE)
        mock_message.receipt_handle = "test_receipt_handle"
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        mock_boto3_resource.return_value = mock_sqs

        connection = SqsConnection(
            aws_access_key_id=TEST_AWS_KEY_ID,
            aws_access_key_secret=TEST_AWS_KEY_SECRET,
            region_name=TEST_REGION,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        callback = Mock(side_effect=ValueError("Test error"))
        # Should not raise, should handle exception internally
        connection.consume_messages(TEST_QUEUE_NAME, callback)

        # Should still delete message even after exception
        mock_queue.delete_messages.assert_called_once()


if __name__ == '__main__':
    unittest.main()
