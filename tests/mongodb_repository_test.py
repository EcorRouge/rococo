"""
Extended MongoDbRepository test cases
"""

import json
import unittest
from uuid import UUID
from unittest.mock import MagicMock
from .base_repository_test import TestVersionedModel
from rococo.repositories.mongodb.mongodb_repository import MongoDbRepository


class MongoDbRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        """
        Set up the test fixture.

        This sets up a mock database adapter, message adapter, and a test model
        instance. The MongoDbRepository is then initialized with these mocks.
        """
        self.db_adapter_mock = MagicMock()
        self.message_adapter_mock = MagicMock()
        self.queue_name = "test_queue"
        self.model_data = {"entity_id": UUID(int=8)}
        self.model_instance = TestVersionedModel(**self.model_data)

        self.repository = MongoDbRepository(
            db_adapter=self.db_adapter_mock,
            model=TestVersionedModel,
            message_adapter=self.message_adapter_mock,
            queue_name=self.queue_name
        )

    def test_save_sends_message(self):
        """
        Tests that the save method sends a message to the message queue after saving.

        Verifies that the save method sends a message to the message queue with the
        saved entity data and the correct queue name. Also verifies that the saved
        instance is returned with the correct entity_id.
        """
        # For this test, assume save might reconstruct, so mock adapter.save's return value
        self.db_adapter_mock.save.return_value = self.model_instance.as_dict()
        self.db_adapter_mock.move_entity_to_audit_table.return_value = None
        self.db_adapter_mock.get_many.return_value = []

        saved_instance = self.repository.save(
            self.model_instance, "test_collection", send_message=True)

        self.message_adapter_mock.send_message.assert_called_once_with(
            self.queue_name,
            json.dumps(self.model_instance.as_dict(
                convert_datetime_to_iso_string=True))
        )
        self.assertEqual(saved_instance.entity_id,
                         self.model_instance.entity_id)

    def test_save_without_message(self):
        """
        Tests that the save method doesn't send a message to the message queue when
        send_message=False is passed.

        Verifies that the save method doesn't send a message to the message queue
        when send_message=False is passed. Also verifies that the saved
        instance is returned with the correct entity_id.
        """
        self.db_adapter_mock.save.return_value = self.model_instance.as_dict()
        self.db_adapter_mock.move_entity_to_audit_table.return_value = None
        self.db_adapter_mock.get_many.return_value = []

        self.repository.save(self.model_instance,
                             "test_collection", send_message=False)

        self.message_adapter_mock.send_message.assert_not_called()

    def test_delete_sets_active_false(self):
        """
        Tests that the delete method sets the active flag of the instance to False.

        Verifies that the delete method sets the active flag of the instance to False
        and calls the save method. This test ensures that the delete method correctly
        marks the instance as deleted.
        """
        # If repository.save reconstructs the instance from adapter.save's return value,
        # we need adapter.save to be mocked to return data where active is False.
        data_representing_deleted_state = self.model_instance.as_dict().copy()
        data_representing_deleted_state['active'] = False
        self.db_adapter_mock.save.return_value = data_representing_deleted_state
        self.db_adapter_mock.move_entity_to_audit_table.return_value = None

        deleted_instance = self.repository.delete(
            self.model_instance, "test_collection")

        self.assertFalse(deleted_instance.active)
        # Verifies that the save method on the adapter was called
        self.db_adapter_mock.save.assert_called()

    def test_create_sets_active_true(self):
        """
        Tests that the create method sets the active flag of the instance to True.

        Verifies that the create method correctly sets the active flag to True
        for the instance being created and ensures that the save method is called
        on the database adapter.
        """
        # Ensure adapter.save returns data reflecting active: True if create method reconstructs
        data_representing_created_state = self.model_instance.as_dict().copy()
        # Explicitly set for clarity
        data_representing_created_state['active'] = True
        self.db_adapter_mock.save.return_value = data_representing_created_state
        self.db_adapter_mock.get_many.return_value = []
        self.model_instance.active = False  # Start with instance as inactive

        created_instance = self.repository.create(
            self.model_instance, "test_collection")

        self.assertTrue(created_instance.active)
        self.db_adapter_mock.save.assert_called()

    def test_create_many_calls_insert_many(self):
        """
        Tests that the create_many method calls the insert_many method with the correct arguments.

        This test verifies that the `create_many` method of the repository class correctly:
        - Calls the `_execute_within_context` method.
        - Passes a function to `_execute_within_context` that, when executed, calls the `insert_many` method of the adapter.
        - Ensures the `insert_many` method is called with the correct collection name and the expected documents.
        - Confirms that all documents have the `active` flag set to True and the correct `_id` values.
        """
        self.repository._execute_within_context = MagicMock()
        instances = [TestVersionedModel(
            entity_id=UUID(int=i)) for i in range(3)]

        # stub insert_many to return a list of dummy docs
        self.db_adapter_mock.insert_many.return_value = [
            {'entity_id': str(instances[0].entity_id), 'active': True},
            {'entity_id': str(instances[1].entity_id), 'active': True},
            {'entity_id': str(instances[2].entity_id), 'active': True},
        ]

        self.repository.create_many(
            instances, collection_name="test_collection")

        # assert that _execute_within_context was called
        self.repository._execute_within_context.assert_called_once()
        func_passed_to_execute = self.repository._execute_within_context.call_args[0][0]

        # executing func should invoke insert_many
        returned_docs = func_passed_to_execute()
        self.assertEqual(len(returned_docs), 3)

        # now inspect the actual call to insert_many
        self.db_adapter_mock.insert_many.assert_called_once()
        called_collection, called_docs = self.db_adapter_mock.insert_many.call_args[0]

        # 1) correct collection
        self.assertEqual(called_collection, "test_collection")
        # 2) right number of docs
        self.assertEqual(len(called_docs), 3)

        # 3) each doc has expected _id and active=True
        expected_ids = [str(inst.entity_id) for inst in instances]
        actual_ids = [doc['entity_id'] for doc in called_docs]
        self.assertCountEqual(actual_ids, expected_ids)

        for doc in called_docs:
            self.assertTrue(doc.get('active', False),
                            "Each inserted doc should be active")

    def test_get_one_returns_instance(self):
        """
        Tests that the get_one method returns an instance of the model when given a matching condition.

        Verifies that the get_one method correctly returns an instance of the model
        when given a matching condition. The condition is specified as a dictionary
        with the key being the field name and the value being the value of the field.
        The method should return an instance of the model with the correct entity_id.
        """
        # The adapter should return data that can be converted back to a model
        # Using a more complete representation based on TestVersionedModel.as_dict()
        mock_return_data = self.model_instance.as_dict()
        self.db_adapter_mock.get_one.return_value = mock_return_data

        result = self.repository.get_one("test_collection", "entity_id_idx", {
                                         "entity_id": str(self.model_instance.entity_id)})
        self.assertIsInstance(result, TestVersionedModel)
        self.assertEqual(result.entity_id, self.model_instance.entity_id)

    def test_get_one_returns_none(self):
        """
        Tests that the get_one method returns None when no matching record is found.

        Verifies that the get_one method correctly returns None when no record is found
        matching the given condition. The condition is specified as a dictionary
        with the key being the field name and the value being the value of the field.
        The method should return None in this case.
        """
        self.db_adapter_mock.get_one.return_value = None

        result = self.repository.get_one("test_collection", "entity_id_idx", {
                                         "entity_id": str(self.model_instance.entity_id)})
        self.assertIsNone(result)

    def test_get_many_returns_instances(self):
        """
        Tests that the get_many method returns a list of instances when given a matching condition.

        Verifies that the get_many method correctly returns a list of instances of the model
        when given a matching condition. The condition is specified as a dictionary
        with the key being the field name and the value being the value of the field.
        The method should return a list of instances of the model with the correct entity_id.
        """
        # Provide more complete data for each instance
        instance1_data = TestVersionedModel(entity_id=UUID(int=1)).as_dict()
        instance2_data = TestVersionedModel(entity_id=UUID(int=2)).as_dict()

        self.db_adapter_mock.get_many.return_value = [
            instance1_data, instance2_data]

        result = self.repository.get_many("test_collection", "entity_id_idx")
        self.assertEqual(len(result), 2)
        for item in result:
            self.assertIsInstance(item, TestVersionedModel)

    def test_get_many_empty(self):
        """
        Tests that the get_many method returns an empty list when no records match the condition.

        Verifies that the get_many method correctly returns an empty list when the database
        adapter returns an empty list, indicating no records match the provided condition.
        """
        self.db_adapter_mock.get_many.return_value = []

        result = self.repository.get_many("test_collection", "entity_id_idx")
        self.assertEqual(result, [])

    def test_get_many_with_offset(self):
        """
        Tests that the get_many method correctly passes the offset parameter to the adapter.

        Verifies that the get_many method correctly passes the offset parameter to the
        database adapter's get_many method when specified. This test ensures that
        pagination functionality works as expected.
        """
        # Provide test data
        instance1_data = TestVersionedModel(entity_id=UUID(int=1)).as_dict()
        instance2_data = TestVersionedModel(entity_id=UUID(int=2)).as_dict()

        self.db_adapter_mock.get_many.return_value = [
            instance1_data, instance2_data]

        # Call get_many with offset parameter
        result = self.repository.get_many(
            "test_collection", "entity_id_idx", limit=10, offset=5)

        # Verify that the adapter's get_many method was called with the correct parameters
        self.db_adapter_mock.get_many.assert_called_once_with(
            table="test_collection",
            conditions={'active': True},
            hint="entity_id_idx",
            limit=10,
            offset=5
        )

        # Verify the result
        self.assertEqual(len(result), 2)
        for item in result:
            self.assertIsInstance(item, TestVersionedModel)

    def test_get_many_with_default_offset(self):
        """
        Tests that the get_many method uses default offset of 0 when not specified.

        Verifies that the get_many method correctly uses the default offset value of 0
        when the offset parameter is not provided, maintaining backward compatibility.
        """
        # Provide test data
        instance1_data = TestVersionedModel(entity_id=UUID(int=1)).as_dict()

        self.db_adapter_mock.get_many.return_value = [instance1_data]

        # Call get_many without offset parameter
        result = self.repository.get_many("test_collection", "entity_id_idx")

        # Verify that the adapter's get_many method was called with offset=0
        self.db_adapter_mock.get_many.assert_called_once_with(
            table="test_collection",
            conditions={'active': True},
            hint="entity_id_idx",
            limit=None,    # default limit (0 means no limit)
            offset=None    # default offset
        )

        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], TestVersionedModel)


if __name__ == '__main__':
    unittest.main()
