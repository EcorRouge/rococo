"""
A connection to a RabbitMQ message queue that allows to send and receive messages.
"""
import json
import functools
import threading
import logging
from typing import Callable
import pika

from . import MessageAdapter

logger = logging.getLogger(__name__)


class RabbitMqConnection(MessageAdapter):
    """A connection to a RabbitMQ message queue that allows to send and receive messages."""

    def __init__(self, host: str, port: int, username: str, password: str, virtual_host: str = ''):
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

        self._connection = None
        self._channel = None
        self._threads = []

    def __enter__(self):
        """
        Opens a connection to the RabbitMQ server.

        Returns:
            RabbitMqConnection: The connection to the RabbitMQ server.
        """
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port,
                                      credentials=pika.PlainCredentials(
                                          self._username,
                                          self._password
                                          ),
                                      virtual_host=self._virtual_host))
        self._channel = self._connection.channel()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Closes the connection to the RabbitMQ server.
        """
        self._connection.close()
        self._connection = None
        self._channel = None

    def send_message(self, queue_name: str, message: dict):
        """
        Sends a message to the specified queue.

        Args:
            queue_name (str): The name of the queue to send the message to.
            message (dict): The message to send.
        """
        self._channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))

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

        def _on_message(ch, method_frame, _header_frame, body, args):
            """Called when a message is received."""

            body = json.loads(body.decode())
            (callback,) = args
            delivery_tag = method_frame.delivery_tag
            t = threading.Thread(
                target=_do_work, args=(ch, delivery_tag, body, callback)
            )
            t.start()
            self._threads.append(t)

        self._channel.queue_declare(queue=queue_name, durable=True)

        self._threads = []
        self._channel.basic_qos(prefetch_count=num_threads)

        on_message_callback = functools.partial(_on_message, args=(callback_function,))
        self._channel.basic_consume(
            queue=queue_name, on_message_callback=on_message_callback
        )

        try:
            logger.info('Listening to RabbitMQ queue %s on %s:%s with %s threads...', queue_name,
                        self._host, self._port, num_threads)
            self._channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Exiting gracefully...")
            self._channel.stop_consuming()
        finally:
            # Wait for all to complete
            for thread in self._threads:
                thread.join()
            self._connection.close()
