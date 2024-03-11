"""
Surreal DB repository test
"""

import json
import unittest
from unittest.mock import MagicMock
from uuid import UUID

from dataclasses import dataclass
from rococo.models.surrealdb import SurrealVersionedModel
from rococo.repositories.surrealdb import SurrealDbRepository


@dataclass
class VersionedModelHelper(SurrealVersionedModel):
    """
    VersionedModelHelper class
    """
    name: str = None
    __test__ = False


class SurrealDbRepositoryTestCase(unittest.TestCase):
    """
    SurrealDbRepository Test Case class
    """
    def setUp(self):
        self.db_adapter_mock = MagicMock()
        self.message_adapter_mock = MagicMock()
        self.model_data = {"entity_id": UUID(int=8), "name": "test"}
        self.model_instance = VersionedModelHelper(**self.model_data)
        self.queue_name = "test_queue"
        self.repository = SurrealDbRepository(
            db_adapter=self.db_adapter_mock,
            model=VersionedModelHelper,
            message_adapter=self.message_adapter_mock,
            queue_name=self.queue_name
        )

    def test_save_sends_message(self):
        """
        test that saving sends the message
        """
        # Set up the mock to return a successful save
        self.db_adapter_mock.save.return_value = True

        # Call the save method
        saved_instance = self.repository.save(self.model_instance, send_message=True)

        surrealdb_dict = saved_instance.as_dict(convert_datetime_to_iso_string=True)
        surrealdb_dict['id'] = surrealdb_dict.pop('entity_id')
        # Assert the save method on the adapter was called once with the correct arguments
        self.db_adapter_mock.save.assert_called_once_with(
            "versionedmodelhelper", {**surrealdb_dict, 'id': f'{self.model_instance.__class__.__name__.lower()}:`{surrealdb_dict["id"]}`'}
        )

        # Assert the send_message method was called once with the correct arguments
        self.message_adapter_mock.send_message.assert_called_once_with(
            self.queue_name, json.dumps(self.model_instance.as_dict(convert_datetime_to_iso_string=True))
        )

    def test_save_without_message(self):
        """
        Test save without sending message
        """
        # Set up the mock to return a successful save
        self.db_adapter_mock.save.return_value = True

        # Call the save method with send_message as False
        self.repository.save(self.model_instance, send_message=False)

        # Assert the save method on the adapter was called
        self.db_adapter_mock.save.assert_called_once()

        # Assert the send_message method was not called
        self.message_adapter_mock.send_message.assert_not_called()


if __name__ == '__main__':
    unittest.main()
