"""
Tests for RabbitMQ message queue connection and operations.

This module tests the RabbitMQ connection class which handles
message sending, consuming, threading, and error recovery.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import threading
import pika
from pika.spec import PERSISTENT_DELIVERY_MODE, TRANSIENT_DELIVERY_MODE

from rococo.messaging.rabbitmq import RabbitMqConnection


# Test constants
TEST_HOST = "localhost"
TEST_PORT = 5672
TEST_USERNAME = "test_user"
TEST_PASSWORD = "test_password"
TEST_VIRTUAL_HOST = "/test"
TEST_QUEUE_NAME = "test_queue"
TEST_MESSAGE = {"key": "value", "id": 123}
TEST_CONFIG_PATH = "/path/to/config.env"


class TestRabbitMqInit(unittest.TestCase):
    """Test RabbitMQ connection initialization."""

    def test_init_stores_connection_parameters(self):
        """
        Test that initialization stores all connection parameters.

        Verifies that host, port, credentials, and virtual host are stored.
        """
        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            virtual_host=TEST_VIRTUAL_HOST,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        self.assertEqual(connection._host, TEST_HOST)
        self.assertEqual(connection._port, TEST_PORT)
        self.assertEqual(connection._username, TEST_USERNAME)
        self.assertEqual(connection._password, TEST_PASSWORD)
        self.assertEqual(connection._virtual_host, TEST_VIRTUAL_HOST)
        self.assertEqual(connection._consume_config_file_path, TEST_CONFIG_PATH)

    def test_init_initializes_connection_as_none(self):
        """
        Test that connection and channel are initialized as None.

        Verifies initial state before connection is established.
        """
        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        self.assertIsNone(connection._connection)
        self.assertIsNone(connection._channel)
        self.assertEqual(connection._threads, {})

    def test_init_without_virtual_host(self):
        """
        Test initialization without virtual host parameter.

        Verifies default empty string for virtual host.
        """
        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        self.assertEqual(connection._virtual_host, '')


class TestRabbitMqConnect(unittest.TestCase):
    """Test RabbitMQ connection establishment and retry logic."""

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_connect_success_on_first_try(self, mock_blocking_connection):
        """
        Test successful connection on first attempt.

        Verifies that connection and channel are properly established.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            virtual_host=TEST_VIRTUAL_HOST
        )

        result = connection._connect()

        self.assertEqual(result, connection)
        self.assertEqual(connection._connection, mock_connection)
        self.assertEqual(connection._channel, mock_channel)
        mock_blocking_connection.assert_called_once()

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_connect_creates_connection_parameters(self, mock_blocking_connection):
        """
        Test that connection creates proper ConnectionParameters.

        Verifies correct host, port, credentials, and virtual_host configuration.
        """
        mock_connection = MagicMock()
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            virtual_host=TEST_VIRTUAL_HOST
        )

        connection._connect()

        call_args = mock_blocking_connection.call_args[0][0]
        self.assertEqual(call_args.host, TEST_HOST)
        self.assertEqual(call_args.port, TEST_PORT)
        self.assertEqual(call_args.virtual_host, TEST_VIRTUAL_HOST)
        self.assertIsInstance(call_args.credentials, pika.PlainCredentials)

    @patch('rococo.messaging.rabbitmq.time.sleep')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_connect_retries_on_connection_error(self, mock_blocking_connection, mock_sleep):
        """
        Test connection retry logic on AMQPConnectionError.

        Verifies that connection retries with 5 second intervals.
        """
        mock_connection = MagicMock()
        # Fail twice, succeed on third attempt
        mock_blocking_connection.side_effect = [
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPConnectionError,
            mock_connection
        ]

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        result = connection._connect(retry_interval=5)

        self.assertEqual(result, connection)
        self.assertEqual(mock_blocking_connection.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(5)

    @patch('rococo.messaging.rabbitmq.time.sleep')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_connect_retries_on_channel_error(self, mock_blocking_connection, mock_sleep):
        """
        Test connection retry logic on AMQPChannelError.

        Verifies that channel errors also trigger retry logic.
        """
        mock_connection = MagicMock()
        mock_blocking_connection.side_effect = [
            pika.exceptions.AMQPChannelError,
            mock_connection
        ]

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        connection._connect()

        self.assertEqual(mock_blocking_connection.call_count, 2)
        mock_sleep.assert_called_once_with(5)


class TestRabbitMqContextManager(unittest.TestCase):
    """Test RabbitMQ context manager protocol (__enter__ and __exit__)."""

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_enter_calls_connect(self, mock_blocking_connection):
        """
        Test that __enter__ calls _connect and returns self.

        Verifies context manager entry.
        """
        mock_connection = MagicMock()
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        result = connection.__enter__()

        self.assertEqual(result, connection)
        self.assertIsNotNone(connection._connection)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_exit_closes_connection_if_open(self, mock_blocking_connection):
        """
        Test that __exit__ closes the connection if it's open.

        Verifies proper cleanup on context exit.
        """
        mock_connection = MagicMock()
        mock_connection.is_open = True
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        connection.__enter__()
        connection.__exit__(None, None, None)

        mock_connection.close.assert_called_once()
        self.assertIsNone(connection._connection)
        self.assertIsNone(connection._channel)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_exit_does_not_close_if_not_open(self, mock_blocking_connection):
        """
        Test that __exit__ handles closed connections gracefully.

        Verifies no error when connection is already closed.
        """
        mock_connection = MagicMock()
        mock_connection.is_open = False
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        connection.__enter__()
        connection.__exit__(None, None, None)

        mock_connection.close.assert_not_called()


class TestRabbitMqSendMessage(unittest.TestCase):
    """Test message sending functionality."""

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_send_message_with_persistent_delivery(self, mock_blocking_connection):
        """
        Test sending a message with persistent delivery mode.

        Verifies that messages are sent with PERSISTENT_DELIVERY_MODE by default.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()

        connection.send_message(TEST_QUEUE_NAME, TEST_MESSAGE, persistent=True)

        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        self.assertEqual(call_args[1]['exchange'], '')
        self.assertEqual(call_args[1]['routing_key'], TEST_QUEUE_NAME)
        self.assertEqual(call_args[1]['body'], json.dumps(TEST_MESSAGE).encode())
        self.assertEqual(call_args[1]['properties'].delivery_mode, PERSISTENT_DELIVERY_MODE)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_send_message_with_transient_delivery(self, mock_blocking_connection):
        """
        Test sending a message with transient delivery mode.

        Verifies that persistent=False uses TRANSIENT_DELIVERY_MODE.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()

        connection.send_message(TEST_QUEUE_NAME, TEST_MESSAGE, persistent=False)

        call_args = mock_channel.basic_publish.call_args
        self.assertEqual(call_args[1]['properties'].delivery_mode, TRANSIENT_DELIVERY_MODE)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_send_message_json_serializes_dict(self, mock_blocking_connection):
        """
        Test that message dict is JSON serialized and encoded.

        Verifies proper message formatting.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()

        message = {"test": "data", "number": 42}
        connection.send_message(TEST_QUEUE_NAME, message)

        expected_body = json.dumps(message).encode()
        call_args = mock_channel.basic_publish.call_args
        self.assertEqual(call_args[1]['body'], expected_body)


class TestRabbitMqConsumeMessages(unittest.TestCase):
    """Test message consumption with threading."""

    @patch('rococo.messaging.rabbitmq.dotenv_values')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_read_consume_config_returns_empty_dict_if_no_path(self, mock_blocking_connection, mock_dotenv):
        """
        Test that _read_consume_config returns empty dict when no path provided.

        Verifies default behavior without config file.
        """
        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        result = connection._read_consume_config()

        self.assertEqual(result, {})
        mock_dotenv.assert_not_called()

    @patch('rococo.messaging.rabbitmq.dotenv_values')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_read_consume_config_loads_from_file(self, mock_blocking_connection, mock_dotenv):
        """
        Test that _read_consume_config loads from file when path provided.

        Verifies config file loading.
        """
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1', 'OTHER_KEY': 'value'}

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            consume_config_file_path=TEST_CONFIG_PATH
        )

        result = connection._read_consume_config()

        self.assertEqual(result, {'EXIT_WHEN_FINISHED': '1', 'OTHER_KEY': 'value'})
        mock_dotenv.assert_called_once_with(TEST_CONFIG_PATH)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_ack_message_threadsafe_acks_if_channel_open(self, mock_blocking_connection):
        """
        Test that _ack_message_threadsafe acknowledges message if channel is open.

        Verifies thread-safe acknowledgment logic.
        """
        mock_channel = MagicMock()
        mock_channel.is_open = True

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        connection._ack_message_threadsafe(mock_channel, 123)

        mock_channel.basic_ack.assert_called_once_with(123)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_ack_message_threadsafe_does_not_ack_if_channel_closed(self, mock_blocking_connection):
        """
        Test that _ack_message_threadsafe does not ack if channel is closed.

        Verifies safe handling of closed channels.
        """
        mock_channel = MagicMock()
        mock_channel.is_open = False

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        connection._ack_message_threadsafe(mock_channel, 123)

        mock_channel.basic_ack.assert_not_called()

    @patch('rococo.messaging.rabbitmq.threading.Thread')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_on_message_received_spawns_thread(self, mock_blocking_connection, mock_thread):
        """
        Test that _on_message_received spawns a thread for processing.

        Verifies threading logic for message processing.
        """
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )

        mock_channel = MagicMock()
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 456
        body = json.dumps(TEST_MESSAGE).encode()

        callback = Mock()

        connection._on_message_received(callback, mock_channel, mock_method_frame, None, body)

        mock_thread.assert_called_once()
        mock_thread_instance.start.assert_called_once()
        self.assertIn(456, connection._threads)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_ensure_connection_does_not_reconnect_if_open(self, mock_blocking_connection):
        """
        Test that _ensure_connection does not reconnect if connection is already open.

        Verifies that unnecessary reconnections are avoided.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_connection.is_open = True
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()

        # Reset mock to track any new connection attempts
        mock_blocking_connection.reset_mock()

        connection._ensure_connection()

        # Should NOT attempt reconnection since connection is open
        mock_blocking_connection.assert_not_called()

    @patch('rococo.messaging.rabbitmq.dotenv_values')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_should_exit_on_inactivity_true_when_exit_flag_set_and_no_threads(
        self, mock_blocking_connection, mock_dotenv
    ):
        """
        Test that _should_exit_on_inactivity returns True when EXIT_WHEN_FINISHED=1 and no threads.

        Verifies exit condition for batch processing.
        """
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            consume_config_file_path=TEST_CONFIG_PATH
        )
        connection._threads = {}

        result = connection._should_exit_on_inactivity()

        self.assertTrue(result)

    @patch('rococo.messaging.rabbitmq.dotenv_values')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_should_exit_on_inactivity_false_when_threads_running(
        self, mock_blocking_connection, mock_dotenv
    ):
        """
        Test that _should_exit_on_inactivity returns False when threads are running.

        Verifies that consumer waits for threads to complete.
        """
        mock_dotenv.return_value = {'EXIT_WHEN_FINISHED': '1'}

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            consume_config_file_path=TEST_CONFIG_PATH
        )
        connection._threads = {1: Mock()}

        result = connection._should_exit_on_inactivity()

        self.assertFalse(result)

    @patch('rococo.messaging.rabbitmq.dotenv_values')
    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_should_exit_on_inactivity_false_when_exit_flag_not_set(
        self, mock_blocking_connection, mock_dotenv
    ):
        """
        Test that _should_exit_on_inactivity returns False when EXIT_WHEN_FINISHED is not set.

        Verifies continuous consumption mode.
        """
        mock_dotenv.return_value = {}

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            consume_config_file_path=TEST_CONFIG_PATH
        )
        connection._threads = {}

        result = connection._should_exit_on_inactivity()

        self.assertFalse(result)


class TestRabbitMqProcessMessage(unittest.TestCase):
    """Test message processing in thread."""

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_process_message_calls_callback(self, mock_blocking_connection):
        """
        Test that _process_message calls the callback function.

        Verifies callback invocation with message body.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()
        connection._connection = mock_connection

        callback = Mock()
        delivery_tag = 789

        connection._process_message(mock_channel, delivery_tag, TEST_MESSAGE, callback)

        callback.assert_called_once_with(TEST_MESSAGE)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_process_message_acks_after_processing(self, mock_blocking_connection):
        """
        Test that _process_message schedules acknowledgment after processing.

        Verifies that add_callback_threadsafe is called for ack.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()
        connection._connection = mock_connection

        callback = Mock()
        delivery_tag = 789

        connection._process_message(mock_channel, delivery_tag, TEST_MESSAGE, callback)

        mock_connection.add_callback_threadsafe.assert_called_once()

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_process_message_removes_thread_from_dict(self, mock_blocking_connection):
        """
        Test that _process_message removes thread from _threads dict after completion.

        Verifies thread cleanup.
        """
        mock_connection = MagicMock()
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()
        connection._connection = mock_connection

        mock_channel = MagicMock()
        mock_channel.is_open = True
        callback = Mock()
        delivery_tag = 789
        connection._threads[delivery_tag] = Mock()

        connection._process_message(mock_channel, delivery_tag, TEST_MESSAGE, callback)

        self.assertNotIn(delivery_tag, connection._threads)

    @patch('rococo.messaging.rabbitmq.pika.BlockingConnection')
    def test_process_message_handles_callback_exception(self, mock_blocking_connection):
        """
        Test that _process_message handles exceptions in callback gracefully.

        Verifies error handling and that ack still happens.
        """
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_blocking_connection.return_value = mock_connection

        connection = RabbitMqConnection(
            host=TEST_HOST,
            port=TEST_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD
        )
        connection._connect()
        connection._connection = mock_connection

        callback = Mock(side_effect=ValueError("Test error"))
        delivery_tag = 789

        # Should not raise, should handle exception internally
        connection._process_message(mock_channel, delivery_tag, TEST_MESSAGE, callback)

        # Should still attempt to ack
        mock_connection.add_callback_threadsafe.assert_called_once()


if __name__ == '__main__':
    unittest.main()
