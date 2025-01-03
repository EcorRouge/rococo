"""
A connection to a RabbitMQ message queue that allows to send and receive messages.
"""
import json
import functools
import threading
import logging
from typing import Callable
import pika
from dotenv import dotenv_values
import time

from . import MessageAdapter

logger = logging.getLogger(__name__)


class RabbitMqConnection(MessageAdapter):
    """A connection to a RabbitMQ message queue that allows to send and receive messages."""

    def __init__(self, host: str, port: int, username: str, password: str, virtual_host: str = '', consume_config_file_path: str = None):
        """
        Initializes a new RabbitMQ connection.

        Args:
            host (str): The host of the RabbitMQ server.
            port (int): The port of the RabbitMQ server.
            username (str): The username to use when connecting to the RabbitMQ server.
            password (str): The password to use when connecting to the RabbitMQ server.
            virtual_host (str): The virtual host to use when connecting to the RabbitMQ server.
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._virtual_host = virtual_host
        self._consume_config_file_path = consume_config_file_path

        self._connection = None
        self._channel = None
        self._threads = {}

    def __enter__(self):
        """
        Opens a connection to the RabbitMQ server.

        Returns:
            RabbitMqConnection: The connection to the RabbitMQ server.
        """
        return self._connect()

        
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Closes the connection to the RabbitMQ server.
        """
        if self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None


    def _connect(self, retry_interval=5):
        """
        Connects to the RabbitMQ server.
        """

        retry_count = 0
        while True:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self._host, port=self._port,
                                            credentials=pika.PlainCredentials(
                                                self._username,
                                                self._password
                                                ),
                                            virtual_host=self._virtual_host))
                self._channel = self._connection.channel()
                return self
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
                retry_count += 1
                if retry_count % 10 == 0:
                    logger.warning("Unable to connect to RabbitMQ after %s retries...", retry_count)
                    logger.exception(e)
                logging.info("Unable to connect to RabbitMQ after %s retries... Retrying in 5 seconds...", retry_count)
                time.sleep(retry_interval)


    def _read_consume_config(self):
        if self._consume_config_file_path is None:
            return {}
        return dotenv_values(self._consume_config_file_path)

    def send_message(self, queue_name: str, message: dict, persistent: bool = True):
        """
        Sends a message to the specified queue.

        Args:
            queue_name (str): The name of the queue to send the message to.
            message (dict): The message to send.
            persistent (bool): If True, the message is persisted to disk. Defaults to True.
        """

        from pika.spec import PERSISTENT_DELIVERY_MODE, TRANSIENT_DELIVERY_MODE

        delivery_mode = PERSISTENT_DELIVERY_MODE if persistent else TRANSIENT_DELIVERY_MODE

        self._channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode
            )
        )

    def consume_messages(self, queue_name: str,
                         callback_function: Callable[[dict], bool],
                         num_threads: int = 1):
        """
        Consumes messages from the specified queue.

        Args:
            queue_name (str): The name of the queue to consume messages from.
            callback_function (callable): The function to call when a message is received.
            num_threads (int): number of threads
        """

        def _ack_message(ch, delivery_tag):
            """Ack a message by its delivery tag if channel is open."""

            if ch.is_open:
                ch.basic_ack(delivery_tag)

        def _do_work(ch, delivery_tag, body, callback):
            """Callback function that processes the message."""

            thread_id = threading.get_ident()
            logger.info(
                "Thread id: %s Delivery tag: %s Message body: %s",
                thread_id,
                delivery_tag,
                body,
            )
            try:
                callback(body)
            except Exception:  # pylint: disable=W0718
                logger.exception("Error processing message...")
            logger.info(
                "Thread id: %s Delivery tag: %s Message body: %s Processed...",
                thread_id,
                delivery_tag,
                body,
            )
            cb = functools.partial(_ack_message, ch, delivery_tag)
            logger.info("Sent ack for Delivery tag %s...", delivery_tag)
            self._connection.add_callback_threadsafe(cb)
            self._threads.pop(delivery_tag, None)

        def _on_message(ch, method_frame, _header_frame, body, args):
            """Called when a message is received."""

            body = json.loads(body.decode())
            (callback,) = args
            delivery_tag = method_frame.delivery_tag
            t = threading.Thread(
                target=_do_work, args=(ch, delivery_tag, body, callback)
            )
            t.start()
            self._threads[delivery_tag] = t

        self._threads = {}
        self._channel.basic_qos(prefetch_count=num_threads)

        on_message_callback = functools.partial(_on_message, args=(callback_function,))

        while True:
            try:
                if not self._channel.is_open or not self._connection.is_open:
                    logging.info("Reconnecting...")
                    self._connect()
                self._channel.queue_declare(queue=queue_name, durable=True)
                logging.info("Listening to RabbitMQ queue %s on %s:%s with %s threads...", queue_name,
                            self._host, self._port, num_threads)
                for method, properties, body in self._channel.consume(queue=queue_name, inactivity_timeout=5):
                    if (method, properties, body) == (None, None, None):
                        if len(self._threads) == 0 and self._read_consume_config().get('EXIT_WHEN_FINISHED') == '1':
                            logging.info("Reached inactivity timeout. No threads are running and EXIT_WHEN_FINISHED=1, exiting!")
                            break
                    else:
                        on_message_callback(self._channel, method, properties, body)
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as _:
                continue
            except KeyboardInterrupt:
                logging.info("Exiting gracefully...")
                break
            finally:
                # Wait for all to complete
                for thread in self._threads.values():
                    thread.join()

