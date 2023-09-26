import json
import boto3
import logging
import uuid

from . import Connection

logger = logging.getLogger(__name__)

class SqsConnection(Connection):
    """A connection to AWS SQS that allows sending and receiving messages to and from queues."""

    def __init__(self, aws_access_key_id: str = None, aws_access_key_secret: str = None, region_name: str = None):
        """Initializes a new SQS connection.

        Args:
            aws_access_key_id (str): The AWS access key ID.
            aws_access_key_secret (str): The AWS access key secret.
            region_name (str): The AWS region name.
        """

        self._aws_access_key_id = aws_access_key_id
        self._aws_access_key_secret = aws_access_key_secret
        self._region_name = region_name
        self._sqs = boto3.resource('sqs', aws_access_key_id=self._aws_access_key_id, aws_secret_access_key=self._aws_access_key_secret, region_name=self._region_name)
        self._queue_map = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def send_message(self, queue_name: str, message: dict):
        """Sends a message to the specified SQS queue.
        
        Args:
            queue_name (str): The name of the queue to send the message to.
            message (dict): The message to send.
        """
        if queue_name in self._queue_map:
            queue = self._queue_map[queue_name]
        else:
            queue = self._sqs.get_queue_by_name(QueueName=queue_name)

        queue.send_message(QueueUrl=queue_name, MessageBody=json.dumps(message))

    def consume_messages(self, queue_name: str, callback_function: callable):
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

        logger.info(f"Connecting to SQS queue: {queue_name}...")
        queue = self._sqs.get_queue_by_name(QueueName=queue_name)

        while True:
            logger.info(f"Fetching messages from SQS queue: {queue_name}...")
            responses = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            if not responses:
                logger.info("No messages left in queue.")
                continue

            response = {}
            try:
                response = responses[0]
                body = json.loads(response.body)
                callback_function(body)
            except Exception as _:
                logger.exception("Error processing message...")
            _delete_queue_message(queue, response.receipt_handle)
