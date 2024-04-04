"""A connection to AWS SQS that allows sending and receiving messages to and from queues."""
import json
import logging
import uuid
import boto3
from dotenv import dotenv_values

from . import MessageAdapter

logger = logging.getLogger(__name__)


class SqsConnection(MessageAdapter):
    """A connection to AWS SQS that allows sending and receiving messages to and from queues."""

    def __init__(self, aws_access_key_id: str = None,
                 aws_access_key_secret: str = None,
                 region_name: str = None,
                 consume_config_file_path: str = None):
        """Initializes a new SQS connection.

        Args:
            aws_access_key_id (str): The AWS access key ID.
            aws_access_key_secret (str): The AWS access key secret.
            region_name (str): The AWS region name.
        """

        self._aws_access_key_id = aws_access_key_id
        self._aws_access_key_secret = aws_access_key_secret
        self._region_name = region_name
        self._consume_config_file_path = consume_config_file_path
        self._sqs = boto3.resource('sqs',
                                   aws_access_key_id=self._aws_access_key_id,
                                   aws_secret_access_key=self._aws_access_key_secret,
                                   region_name=self._region_name)
        self._queue_map = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _read_consume_config(self):
        if self._consume_config_file_path is None:
            return {}
        return dotenv_values(self._consume_config_file_path)

    def send_message(self, queue_name: str, message: dict):
        """Sends a message to the specified SQS queue.
        
        Args:
            queue_name (str): The name of the queue to send the message to.
            message (dict): The message to send.
        """
        if queue_name in self._queue_map:
            queue = self._queue_map[queue_name]
        else:
            queue = self._sqs.create_queue(QueueName=queue_name)

        queue.send_message(QueueUrl=queue_name, MessageBody=json.dumps(message))

    def consume_messages(self, queue_name: str, callback_function: callable = None):
        """Consumes messages from the specified SQS queue.

        Args:
            queue_name (str): The name of the queue to consume messages from.
            callback_function (callable): The function to call when a message is received.
        """

        def _delete_queue_message(queue, handle):
            logger.info("Deleting message.")
            queue.delete_messages(
                Entries=[
                    {
                        'Id': str(uuid.uuid4()),
                        'ReceiptHandle': handle
                    },
                ]
            )

        logger.info("Connecting to SQS queue: %s...", queue_name)
        queue = self._sqs.create_queue(QueueName=queue_name)

        while True:
            logger.info("Fetching messages from SQS queue: %s...", queue_name)
            responses = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            if not responses:
                logger.info("No messages left in queue.")
                if self._read_consume_config().get('EXIT_WHEN_FINISHED') == '1':
                    logger.info("EXIT_WHEN_FINISHED=1 and no messages left in queue. Exiting...")
                    return
                continue

            response = {}
            try:
                response = responses[0]
                body = json.loads(response.body)
                if callback_function is not None:
                    callback_function(body)
            except Exception as _:  # pylint: disable=W0718
                logger.exception("Error processing message...")
            _delete_queue_message(queue, response.receipt_handle)
