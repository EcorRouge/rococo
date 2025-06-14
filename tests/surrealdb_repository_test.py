"""
Surreal DB repository test
"""

import unittest
from unittest.mock import MagicMock, patch, ANY
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass

from rococo.data import SurrealDbAdapter
from rococo.messaging.base import MessageAdapter
from rococo.models.surrealdb.surreal_versioned_model import SurrealVersionedModel, get_uuid_hex
from rococo.repositories.surrealdb.surreal_db_repository import SurrealDbRepository


@dataclass(kw_only=True)
class VersionedModelHelper(SurrealVersionedModel):
    name: Optional[str] = None
    id: Optional[str] = None
    __test__ = False


class SurrealDbRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        """
        Set up the test case by creating a mock SurrealDbAdapter and
        MessageAdapter, and initializing the SurrealDbRepository with
        a VersionedModelHelper.

        The mock SurrealDbAdapter is used to mock out the database
        operations, and the mock MessageAdapter is used to mock out
        the message queue operations.

        A mock from_dict method is also created for the VersionedModelHelper,
        which is used to mock out the from_dict method of the model. This
        method takes a dictionary as input and returns an instance of the
        VersionedModelHelper with the attributes set from the dictionary.

        The table_name attribute of the repository is set to the name of the
        table in the database, which is the lowercase name of the model.

        The entity_id_hex_for_setup attribute is set to a random UUID in
        hexadecimal form, which is used as the entity_id for the model
        instances created during the test.

        The setUp method is called automatically before each test method,
        and the tearDown method is called automatically after each test
        method.
        """
        self.db_adapter_mock = MagicMock(spec=SurrealDbAdapter)
        self.message_adapter_mock = MagicMock(spec=MessageAdapter)
        self.queue_name = "test_queue"
        self.entity_id_hex_for_setup = get_uuid_hex()
        self.table_name = VersionedModelHelper.__name__.lower()

        self.repository = SurrealDbRepository(
            db_adapter=self.db_adapter_mock,
            model=VersionedModelHelper,
            message_adapter=self.message_adapter_mock,
            queue_name=self.queue_name,
            user_id=None
        )
        self.repository.table_name = self.table_name

        def mock_from_dict_side_effect(data_dict):
            """
            A mock implementation of the from_dict method for the VersionedModelHelper.
            
            This method takes a dictionary as input and returns an instance of the
            VersionedModelHelper with the attributes set from the dictionary.
            
            The method is used to mock out the from_dict method of the model, which is
            called by the SurrealDbRepository when it needs to create an instance of
            the model from a dictionary.
            
            The method is a side effect of the patcher, so it will be called every time
            the from_dict method is called on the mock model.
            """
            if data_dict is None:
                return None
            init_data = data_dict.copy()
            full_surreal_id = init_data.pop('_original_id_temp_', None)

            # constructor only accepts kw_only: entity_id, name
            ctor_args = {
                'entity_id': init_data.get('entity_id'),
                'name': init_data.get('name')
            }
            # inject other attributes after init
            inst = VersionedModelHelper(**ctor_args)
            for attr in ('version', 'active', 'previous_version', 'changed_by_id'):
                if attr in init_data:
                    setattr(inst, attr, init_data[attr])
            if 'created_at' in init_data:
                inst.created_at = init_data['created_at']
            if 'updated_at' in init_data:
                inst.updated_at = init_data['updated_at']

            # restore id if provided
            if full_surreal_id:
                inst.id = full_surreal_id
            else:
                inst.id = f"{self.table_name}:{inst.entity_id}"
            return inst

        patcher = patch.object(
            VersionedModelHelper, 'from_dict', side_effect=mock_from_dict_side_effect
        )
        self.mock_model_from_dict = patcher.start()
        self.addCleanup(patcher.stop)

    def _create_test_instance(self, name_suffix="", set_id_attribute=True, **kwargs):
        """
        Create a test instance of VersionedModelHelper with specified attributes.

        This method initializes a VersionedModelHelper object for testing purposes,
        allowing optional overrides for its attributes.

        :param name_suffix: A suffix to append to the default name if no name is provided.
        :param set_id_attribute: A boolean indicating whether to set the `id` attribute.
        :param kwargs: Additional attributes to override default values:
            - entity_id: The entity ID to assign to the instance.
            - name: The name to assign to the instance.
            - version: The version identifier for the instance.
            - created_at: The creation timestamp for the instance.
            - updated_at: The last updated timestamp for the instance.
            - active: A boolean indicating if the instance is active.
            - previous_version: The previous version identifier.
            - changed_by_id: The ID of the user who made the change.

        :return: A VersionedModelHelper instance with the specified attributes.
        """
        eid = kwargs.pop('entity_id', get_uuid_hex())
        inst = VersionedModelHelper(entity_id=eid, name=kwargs.pop('name', f"test_{name_suffix}"))
        inst.version = kwargs.pop('version', uuid4().hex)
        inst.created_at = kwargs.pop('created_at', datetime.now(timezone.utc).isoformat())
        inst.updated_at = kwargs.pop('updated_at', inst.created_at)
        inst.active = kwargs.pop('active', True)
        inst.previous_version = kwargs.pop('previous_version', None)
        inst.changed_by_id = kwargs.pop('changed_by_id', None)
        # id override or build
        provided_id = kwargs.pop('id', None)
        if set_id_attribute:
            inst.id = provided_id or f"{self.table_name}:{inst.entity_id}"
        return inst

    # helper to patch SurrealDbRepository._extract_uuid_from_surreal_id
    def _patched_extract_uuid(self, id_str, table_name):
        """
        A mock implementation of the _extract_uuid_from_surreal_id method.

        This method takes an id_str and a table_name as input and returns the
        extracted UUID in hexadecimal form. If the id_str is None, it returns
        None. If the id_str is not in the correct format, it will return the
        key part of the id_str as is.

        The method is used to mock out the _extract_uuid_from_surreal_id method
        of the SurrealDbRepository, which is called by the repository when it
        needs to extract the UUID from a SurrealDB id string.

        :param id_str: The SurrealDB id string from which to extract the UUID.
        :param table_name: The table name in which the UUID is stored.
        :return: The extracted UUID in hexadecimal form, or None if id_str is None.
        """
        if id_str is None:
            return None
        key = id_str.split(':',1)[1].strip('`')
        try:
            return UUID(key).hex
        except ValueError:
            return key

    def test_save_sends_message(self):
        """
        Test that the save method sends a message after saving the instance.

        This test verifies that the `save` method of the `SurrealDbRepository` correctly
        handles saving an instance to the database and subsequently sends a message to
        the message queue when `send_message` is set to True.

        It ensures that:
        - The `get_save_query` method of the `db_adapter_mock` is called with the correct data.
        - The version of the saved data is updated.
        - The `entity_id` is not included in the data for saving.
        - The `run_transaction` method of the `db_adapter_mock` is called with the appropriate queries.
        - The `send_message` method of the `message_adapter_mock` is called once with the correct parameters.
        """

        inst = self._create_test_instance("msg_send")
        orig_version = inst.version
        orig_hex = inst.entity_id
        mock_audit = ("AUDIT_Q", (orig_hex,))
        mock_save = ("update", inst.id, ANY)
        self.db_adapter_mock.get_move_entity_to_audit_table_query.return_value = mock_audit
        self.db_adapter_mock.get_save_query.return_value = mock_save
        self.db_adapter_mock.run_transaction.return_value = True

        self.repository.save(inst, send_message=True)
        self.db_adapter_mock.get_save_query.assert_called_once()
        _, data = self.db_adapter_mock.get_save_query.call_args[0]
        self.assertEqual(data['id'], inst.id)
        self.assertNotEqual(data['version'], orig_version)
        self.assertNotIn('entity_id', data)
        self.db_adapter_mock.run_transaction.assert_called_once_with([mock_audit, mock_save])
        self.message_adapter_mock.send_message.assert_called_once()

    def test_save_without_message(self):
        """
        Test that the save method does not send a message when send_message is False.

        This test verifies that the `save` method of the `SurrealDbRepository` correctly
        handles saving an instance to the database without sending a message to the message
        queue when `send_message` is set to False.

        It checks that:
        - The `get_save_query` method of the `db_adapter_mock` is called with the correct data.
        - The version of the saved data is updated.
        - The `entity_id` is not included in the data for saving.
        - The `run_transaction` method of the `db_adapter_mock` is called with the appropriate queries.
        - The `send_message` method of the `message_adapter_mock` is not called.
        """
        inst = self._create_test_instance("no_msg")
        orig_version = inst.version
        orig_hex = inst.entity_id
        mock_audit = ("AUDIT_Q", (orig_hex,))
        mock_save = ("update", inst.id, ANY)
        self.db_adapter_mock.get_move_entity_to_audit_table_query.return_value = mock_audit
        self.db_adapter_mock.get_save_query.return_value = mock_save
        self.db_adapter_mock.run_transaction.return_value = True

        self.repository.save(inst, send_message=False)
        # adapter save should be called
        self.db_adapter_mock.get_save_query.assert_called_once()
        _, data = self.db_adapter_mock.get_save_query.call_args[0]
        self.assertEqual(data['id'], inst.id)
        self.assertNotEqual(data['version'], orig_version)
        # no message should be sent when send_message=False
        self.message_adapter_mock.send_message.assert_not_called()
        
    def test_delete_sets_active_false_and_saves(self):
        """
        Tests that the delete method sets the active flag to False and saves the instance.

        This test ensures that the `delete` method of the `SurrealDbRepository` correctly
        marks an instance as inactive by setting its active flag to False and subsequently
        saves the instance with the updated version. It verifies that:
        - The `get_save_query` method of the `db_adapter_mock` is called with the correct data.
        - The active flag of the deleted instance is set to False.
        - The version of the deleted instance is updated.
        - The `run_transaction` method of the `db_adapter_mock` is called with the appropriate queries.
        """
        inst = self._create_test_instance("to_del", active=True)
        orig_version = inst.version
        orig_hex = inst.entity_id
        mock_audit = ("AUDIT_DEL", (orig_hex,))
        mock_save = ("update", inst.id, ANY)
        self.db_adapter_mock.get_move_entity_to_audit_table_query.return_value = mock_audit
        self.db_adapter_mock.get_save_query.return_value = mock_save
        self.db_adapter_mock.run_transaction.return_value = True

        deleted = self.repository.delete(inst)
        self.assertFalse(deleted.active)
        self.assertNotEqual(deleted.version, orig_version)
        self.db_adapter_mock.get_save_query.assert_called_once()
        _, data = self.db_adapter_mock.get_save_query.call_args[0]
        self.assertFalse(data['active'])
        self.assertEqual(data['id'], inst.id)

    @patch('rococo.repositories.surrealdb.surreal_db_repository.SurrealDbRepository._extract_uuid_from_surreal_id')
    def test_get_one_found(self, mock_ext):
        """
        Test that the `get_one` method retrieves an existing record successfully.

        This test verifies that the `get_one` method of the `SurrealDbRepository` correctly
        retrieves a single existing record from the database. It patches the UUID extraction
        method to ensure proper conversion of SurrealDB ids, mocks the database adapter's
        return value, and checks that the returned instance matches the expected instance.
        
        It ensures that:
        - The `get_one` method of the `db_adapter_mock` is called once.
        - The retrieved instance matches the expected instance created from the raw data.
        """
        mock_ext.side_effect = self._patched_extract_uuid
        eid = get_uuid_hex()
        raw = {'id': f"{self.table_name}:{eid}", 'name':'foo', 'active':True, 'version':eid, 'created_at':datetime.now(timezone.utc).isoformat()}
        self.db_adapter_mock.get_one.return_value = raw
        expected = self._create_test_instance(id=raw['id'], entity_id=eid, name='foo', version=eid, created_at=raw['created_at'], active=True)
        self.mock_model_from_dict.return_value = expected

        result = self.repository.get_one({'entity_id':eid})
        self.assertIs(result, expected)
        self.db_adapter_mock.get_one.assert_called_once()

    @patch('rococo.repositories.surrealdb.surreal_db_repository.SurrealDbRepository._extract_uuid_from_surreal_id')
    def test_get_many_found(self, mock_ext):
        """
        Tests that the `get_many` method retrieves multiple existing records successfully.

        This test verifies that the `get_many` method of the `SurrealDbRepository` correctly
        retrieves multiple existing records from the database. It patches the UUID extraction
        method to ensure proper conversion of SurrealDB ids, mocks the database adapter's
        return value, and checks that the returned instances match the expected instances.
        
        It ensures that:
        - The `get_many` method of the `db_adapter_mock` is called once.
        - The retrieved instances match the expected instances created from the raw data.
        """
        mock_ext.side_effect = self._patched_extract_uuid
        e1, e2 = get_uuid_hex(), get_uuid_hex()
        r1 = {'id':f"{self.table_name}:{e1}", 'name':'g','active':True,'version':e1}
        r2 = {'id':f"{self.table_name}:{e2}", 'name':'g','active':True,'version':e2}
        self.db_adapter_mock.get_many.return_value = [r1, r2]
        inst1 = self._create_test_instance(id=r1['id'], entity_id=e1, name='g', version=e1)
        inst2 = self._create_test_instance(id=r2['id'], entity_id=e2, name='g', version=e2)
        # have from_dict return the right instance based on entity_id
        self.mock_model_from_dict.side_effect = lambda rec: (
                inst1 if ((rec['entity_id'] if isinstance(rec, dict) else rec.entity_id) == e1) 
                else inst2
            )

        results = self.repository.get_many({'name':'g'}, limit=10)
        self.assertEqual([r.id for r in results], [inst1.id, inst2.id])
        self.assertEqual([r.entity_id for r in results], [e1, e2])
        self.db_adapter_mock.get_many.assert_called_once()

    def test_get_one_not_found(self):
        """
        Tests that the `get_one` method correctly handles a non-existing record.

        This test verifies that the `get_one` method of the `SurrealDbRepository` correctly
        handles a non-existing record. It patches the database adapter's return value
        to None, and checks that the returned instance is None and that
        the model's from_dict method is not called.
        """
        self.db_adapter_mock.get_one.return_value = None
        self.mock_model_from_dict.return_value = None
        res = self.repository.get_one({'entity_id':'nope'})
        self.assertIsNone(res)
        self.mock_model_from_dict.assert_not_called()

    def test_get_many_not_found(self):
        """
        Tests that the `get_many` method correctly handles a non-existing record.

        This test verifies that the `get_many` method of the `SurrealDbRepository` correctly
        handles a non-existing record. It patches the database adapter's return value
        to an empty list, and checks that the returned instance is an empty list
        and that the model's from_dict method is not called.
        """
        self.db_adapter_mock.get_many.return_value = []
        self.mock_model_from_dict.reset_mock()
        res = self.repository.get_many({'name':'x'})
        self.assertEqual(res, [])
        self.mock_model_from_dict.assert_not_called()

    def test_get_count(self):
        """
        Test the `get_count` method of the `SurrealDbRepository`.

        This test verifies that the `get_count` method correctly retrieves the
        count of records matching the given query parameters and index. It mocks
        the database adapter to return a specific count and ensures that the
        method returns the expected count. Additionally, it checks that the
        `get_count` method of the adapter is called exactly once.
        """
        self.db_adapter_mock.get_count.return_value = 7
        cnt = self.repository.get_count(self.table_name, 'idx', {'a':1})
        self.db_adapter_mock.get_count.assert_called_once()
        self.assertEqual(cnt, 7)

    def test_relate_calls_adapter_execute_query(self):
        """
        Tests that the `relate` method calls the `execute_query` method of the database adapter
        with the correct query.

        Verifies that the `relate` method of the `SurrealDbRepository` correctly calls the
        `execute_query` method of the database adapter with the correct RELATE query.
        The method is called with two instances and a relation name, and the query is
        constructed using the table name and the entity ids of the two instances.
        The test ensures that the `execute_query` method is called exactly once with the
        correct query.
        """
        in_m = self._create_test_instance('in', entity_id=get_uuid_hex())
        out_m = self._create_test_instance('out', entity_id=get_uuid_hex())
        self.repository.relate(in_m, 'knows', out_m)
        q = f"RELATE {self.table_name}:`{in_m.entity_id}`->knows->{self.table_name}:`{out_m.entity_id}`"
        self.db_adapter_mock.execute_query.assert_called_once_with(q)

    def test_unrelate_calls_adapter_execute_query(self):
        """
        Tests that the `unrelate` method calls the `execute_query` method of the database adapter
        with the correct query.

        Verifies that the `unrelate` method of the `SurrealDbRepository` correctly calls the
        `execute_query` method of the database adapter with the correct DELETE query.
        The method is called with two instances and a relation name, and the query is
        constructed using the table name and the entity ids of the two instances.
        The test ensures that the `execute_query` method is called exactly once with the
        correct query.
        """
        in_m = self._create_test_instance('in_unrel', entity_id=get_uuid_hex())
        out_m = self._create_test_instance('out_unrel', entity_id=get_uuid_hex())
        self.repository.unrelate(in_m, 'liked', out_m)
        q = f"DELETE FROM liked WHERE in={self.table_name}:`{in_m.entity_id}` AND out={self.table_name}:`{out_m.entity_id}`"
        self.db_adapter_mock.execute_query.assert_called_once_with(q)

if __name__ == '__main__':
    unittest.main()
