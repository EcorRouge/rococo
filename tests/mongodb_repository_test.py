"""
Mongo DB repository test
"""

import json
import unittest
from unittest.mock import MagicMock
from uuid import UUID
from .base_repository_test import TestVersionedModel
from rococo.repositories.mongodb.mongodb_repository import MongoDbRepository


class MongoDbRepositoryTestCase(unittest.TestCase):
    """
    MongoDbRepository Test Case class
    """
    def setUp(self):
        self.db_adapter_mock = MagicMock()
        self.message_adapter_mock = MagicMock()
        self.model_data = {"entity_id": UUID(int=8)}
        self.model_instance = TestVersionedModel(**self.model_data)
        self.queue_name = "test_queue"
        self.repository = MongoDbRepository(
            db_adapter=self.db_adapter_mock,
            model=TestVersionedModel,
            message_adapter=self.message_adapter_mock,
            queue_name=self.queue_name
        )

    def test_save_sends_message(self):
        """
        test that saving sends the message
        """
        # Set up the mock to return a successful save
        self.db_adapter_mock.get_save_query.return_value = "", ()
        self.db_adapter_mock.get_move_entity_to_audit_table_query.return_value = "", ()
        self.db_adapter_mock.run_transaction.return_value = True

        # Call the save method
        saved_instance = self.repository.save(self.model_instance, send_message=True)

        mongo_dict = saved_instance
        mongo_dict._id = mongo_dict.entity_id

        # Assert the send_message method was called once with the correct arguments
        self.message_adapter_mock.send_message.assert_called_once_with(
            self.queue_name, json.dumps(self.model_instance.as_dict(convert_datetime_to_iso_string=True))
        )

    def test_save_without_message(self):
        """
        Test save without sending message
        """
        # Set up the mock to return a successful save
        self.db_adapter_mock.get_save_query.return_value = "", ()
        self.db_adapter_mock.get_move_entity_to_audit_table_query.return_value = "", ()
        self.db_adapter_mock.run_transaction.return_value = True

        # Call the save method with send_message as False
        self.repository.save(self.model_instance, send_message=False)

        # Assert the send_message method was not called
        self.message_adapter_mock.send_message.assert_not_called()


if __name__ == '__main__':
    unittest.main()
