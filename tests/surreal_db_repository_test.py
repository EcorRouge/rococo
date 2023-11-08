import json
import unittest
from unittest.mock import MagicMock

from rococo.models import VersionedModel
from rococo.repositories import SurrealDbRepository


class VersionedModelHelper(VersionedModel):
    __test__ = False

    def __init__(self, data):
        super().__init__()
        for key, value in data.items():
            setattr(self, key, value)


class SurrealDbRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self.db_adapter_mock = MagicMock()
        self.message_adapter_mock = MagicMock()
        self.model_data = {"id": 1, "name": "test"}
        self.model_instance = VersionedModelHelper(self.model_data)
        self.queue_name = "test_queue"
        self.repository = SurrealDbRepository(
            db_adapter=self.db_adapter_mock,
            model=VersionedModelHelper,
            message_adapter=self.message_adapter_mock,
            queue_name=self.queue_name
        )

    def test_save_sends_message(self):
        # Set up the mock to return a successful save
        self.db_adapter_mock.save.return_value = True

        # Call the save method
        self.repository.save(self.model_instance)

        # Assert the save method on the adapter was called once with the correct arguments
        self.db_adapter_mock.save.assert_called_once_with(
            "versionedmodelhelper", self.model_instance.as_dict()
        )

        # Assert the send_message method was called once with the correct arguments
        self.message_adapter_mock.send_message.assert_called_once_with(
            self.queue_name, json.dumps(self.model_instance.as_dict(convert_datetime_to_iso_string=True))
        )

    def test_save_without_message(self):
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
