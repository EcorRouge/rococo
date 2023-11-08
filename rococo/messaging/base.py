"""
    An abstract Connection class that enforces the implementation
    of the 'send_message' and 'consume_messages' methods.
"""
from abc import abstractmethod


class MessageAdapter:
    """Abstract class for a connection to a message queue."""

    def __init__(self):
        pass

    @abstractmethod
    def send_message(self, queue_name: str, message: dict):
        """
        Sends a message to the specified queue.

        Args:
            queue_name (str): The name of the queue to send the message to.
            message (dict): The message to send.
        """

    @abstractmethod
    def consume_messages(self, queue_name: str, callback_function: callable = None):
        """
        Consumes messages from the specified queue.

        Args:
            queue_name (str): The name of the queue to consume messages from.
            callback_function (callable): The function to call when a message is received.
        """

    @abstractmethod
    def __enter__(self):
        """Performs any initialization required for the connection."""

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Performs any cleanup required for the connection."""


class BaseServiceProcessor:  # pylint: disable=R0903
    """Abstract class for processing data from a message queue."""

    def __init__(self):
        pass

    @abstractmethod
    def process(self, message):
        """
        Processes the message data.
        """
