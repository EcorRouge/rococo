"""Module for messaging"""
from .base import MessageAdapter
from .rabbitmq import RabbitMqConnection
from .sqs import SqsConnection
from .base import BaseServiceProcessor
