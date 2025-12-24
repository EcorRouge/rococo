"""
Shared pytest fixtures and configuration for database integration tests.

This module provides:
- Mock message adapter for testing without actual message queue
- Database configurations from environment variables
- Skip helpers for unavailable databases
"""

import os
import pytest
from unittest.mock import MagicMock
from uuid import uuid4


from rococo.messaging.base import MessageAdapter


def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers", "known_failure: mark test as known failure that exposes documented bugs"
    )


class MockMessageAdapter(MessageAdapter):
    """Mock message adapter for testing."""
    
    def __init__(self):
        self.messages = []
        self.send_message = MagicMock(side_effect=self._capture_message)
    
    def _capture_message(self, queue_name: str, message: str):
        """Capture sent messages for later verification."""
        self.messages.append({
            'queue_name': queue_name,
            'message': message
        })
    
    def get_messages(self, queue_name: str = None):
        """Get captured messages, optionally filtered by queue name."""
        if queue_name:
            return [m for m in self.messages if m['queue_name'] == queue_name]
        return self.messages
    
    def clear_messages(self):
        """Clear all captured messages."""
        self.messages.clear()
        self.send_message.reset_mock()


@pytest.fixture
def mock_message_adapter():
    """Provide a mock message adapter for tests."""
    return MockMessageAdapter()


@pytest.fixture
def test_user_id():
    """Provide a consistent test user ID for changed_by_id fields."""
    return uuid4()


@pytest.fixture
def queue_name():
    """Provide a default queue name for tests."""
    return "test_queue"


# Database configurations from environment variables
def get_mysql_config():
    """
    MySQL config from environment variables.

    Required env vars:
    - MYSQL_HOST
    - MYSQL_PORT
    - MYSQL_USER
    - MYSQL_PASSWORD
    - MYSQL_DATABASE

    Returns None if any required env var is missing.
    """
    required_vars = ['MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    if not all(os.getenv(var) for var in required_vars):
        return None

    return {
        'host': os.getenv('MYSQL_HOST'),
        'port': int(os.getenv('MYSQL_PORT')),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE')
    }


def get_postgres_config():
    """
    PostgreSQL config from environment variables.

    Required env vars:
    - POSTGRES_HOST
    - POSTGRES_PORT
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    - POSTGRES_DATABASE

    Returns None if any required env var is missing.
    """
    required_vars = ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DATABASE']
    if not all(os.getenv(var) for var in required_vars):
        return None

    return {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT')),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DATABASE')
    }


def get_mongodb_config():
    """
    MongoDB config from environment variables.

    Required env vars:
    - MONGODB_HOST
    - MONGODB_PORT
    - MONGODB_DATABASE

    Optional env vars:
    - MONGODB_USER
    - MONGODB_PASSWORD

    Returns None if any required env var is missing.
    """
    required_vars = ['MONGODB_HOST', 'MONGODB_PORT', 'MONGODB_DATABASE']
    if not all(os.getenv(var) for var in required_vars):
        return None

    # Build URI with optional authentication
    user = os.getenv('MONGODB_USER')
    password = os.getenv('MONGODB_PASSWORD')
    host = os.getenv('MONGODB_HOST')
    port = os.getenv('MONGODB_PORT')

    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
    else:
        uri = f"mongodb://{host}:{port}/"

    return {
        'uri': uri,
        'database': os.getenv('MONGODB_DATABASE')
    }


def get_surrealdb_config():
    """
    SurrealDB config from environment variables.

    Required env vars:
    - SURREALDB_HOST
    - SURREALDB_PORT
    - SURREALDB_USER
    - SURREALDB_PASSWORD
    - SURREALDB_NAMESPACE
    - SURREALDB_DATABASE

    Returns None if any required env var is missing.
    """
    required_vars = ['SURREALDB_HOST', 'SURREALDB_PORT', 'SURREALDB_USER', 'SURREALDB_PASSWORD', 'SURREALDB_NAMESPACE', 'SURREALDB_DATABASE']
    if not all(os.getenv(var) for var in required_vars):
        return None

    return {
        'endpoint': f"ws://{os.getenv('SURREALDB_HOST')}:{os.getenv('SURREALDB_PORT')}/rpc",
        'username': os.getenv('SURREALDB_USER'),
        'password': os.getenv('SURREALDB_PASSWORD'),
        'namespace': os.getenv('SURREALDB_NAMESPACE'),
        'database': os.getenv('SURREALDB_DATABASE')
    }


def get_dynamodb_config():
    """
    DynamoDB config from environment variables.

    Required env vars:
    - AWS_REGION
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY

    Optional env vars:
    - DYNAMODB_ENDPOINT_URL (for local DynamoDB)

    Returns None if any required env var is missing.
    """
    required_vars = ['AWS_REGION', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    if not all(os.getenv(var) for var in required_vars):
        return None

    config = {
        'region': os.getenv('AWS_REGION'),
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
    }

    if os.getenv('DYNAMODB_ENDPOINT_URL'):
        config['endpoint_url'] = os.getenv('DYNAMODB_ENDPOINT_URL')

    return config

