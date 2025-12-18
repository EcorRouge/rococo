"""
Tests for DynamoDB database adapter.

This module tests the DynamoDbAdapter class that provides DynamoDB connectivity
and operations using PynamoDB for the rococo framework.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from uuid import UUID

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, BooleanAttribute, NumberAttribute,
    JSONAttribute, ListAttribute
)
from pynamodb.exceptions import DoesNotExist

from rococo.data.dynamodb import DynamoDbAdapter
from rococo.models import VersionedModel


# Test constants
TEST_TABLE = "test_table"
TEST_ENTITY_ID = "test_entity_123"
TEST_REGION = "us-east-1"


# Test model
@dataclass
class TestModel(VersionedModel):
    """Test model for DynamoDB adapter tests."""
    name: Optional[str] = None
    age: Optional[int] = None
    active: Optional[bool] = None
    metadata: Optional[dict] = None
    tags: Optional[list] = None


class TestDynamoDbAdapterInit(unittest.TestCase):
    """Test DynamoDbAdapter initialization and context manager."""

    def test_init(self):
        """Test adapter initialization requires no parameters."""
        adapter = DynamoDbAdapter()
        self.assertIsInstance(adapter, DynamoDbAdapter)

    def test_enter_returns_self(self):
        """Test __enter__ returns adapter instance for context manager."""
        adapter = DynamoDbAdapter()
        result = adapter.__enter__()
        self.assertIs(result, adapter)

    def test_exit_no_cleanup(self):
        """Test __exit__ performs no cleanup (no exceptions)."""
        adapter = DynamoDbAdapter()
        result = adapter.__exit__(None, None, None)
        self.assertIsNone(result)


class TestDynamoDbAdapterMapTypeToAttribute(unittest.TestCase):
    """Test _map_type_to_attribute method."""

    def setUp(self):
        self.adapter = DynamoDbAdapter()

    def test_map_bool_type(self):
        """Test mapping bool type to BooleanAttribute."""
        attr = self.adapter._map_type_to_attribute(bool)
        self.assertIsInstance(attr, BooleanAttribute)
        self.assertFalse(attr.is_hash_key)
        self.assertFalse(attr.is_range_key)

    def test_map_int_type(self):
        """Test mapping int type to NumberAttribute."""
        attr = self.adapter._map_type_to_attribute(int)
        self.assertIsInstance(attr, NumberAttribute)

    def test_map_float_type(self):
        """Test mapping float type to NumberAttribute."""
        attr = self.adapter._map_type_to_attribute(float)
        self.assertIsInstance(attr, NumberAttribute)

    def test_map_dict_type(self):
        """Test mapping dict type to JSONAttribute."""
        attr = self.adapter._map_type_to_attribute(dict)
        self.assertIsInstance(attr, JSONAttribute)

    def test_map_list_type(self):
        """Test mapping list type to ListAttribute."""
        attr = self.adapter._map_type_to_attribute(list)
        self.assertIsInstance(attr, ListAttribute)

    def test_map_str_type(self):
        """Test mapping str type to UnicodeAttribute."""
        attr = self.adapter._map_type_to_attribute(str)
        self.assertIsInstance(attr, UnicodeAttribute)

    def test_map_hash_key(self):
        """Test mapping with hash_key=True."""
        attr = self.adapter._map_type_to_attribute(str, is_hash_key=True)
        self.assertIsInstance(attr, UnicodeAttribute)
        self.assertTrue(attr.is_hash_key)

    def test_map_range_key(self):
        """Test mapping with range_key=True."""
        attr = self.adapter._map_type_to_attribute(str, is_range_key=True)
        self.assertIsInstance(attr, UnicodeAttribute)
        self.assertTrue(attr.is_range_key)


class TestDynamoDbAdapterGeneratePynamoModel(unittest.TestCase):
    """Test _generate_pynamo_model method."""

    @patch.dict('os.environ', {
        'AWS_REGION': 'us-west-2',
        'AWS_ACCESS_KEY_ID': 'test_key',
        'AWS_SECRET_ACCESS_KEY': 'test_secret'
    })
    def test_generate_model_standard_table(self):
        """
        Test generating PynamoDB model for standard table.

        Verifies:
        - Model inherits from pynamodb.models.Model
        - Meta attributes are set from environment
        - entity_id is hash key
        - No range key in standard table
        """
        adapter = DynamoDbAdapter()
        pynamo_model = adapter._generate_pynamo_model(TEST_TABLE, TestModel, is_audit=False)

        # Check class name
        self.assertEqual(pynamo_model.__name__, "PynamoTestModel")

        # Check inheritance
        self.assertTrue(issubclass(pynamo_model, Model))

        # Check Meta attributes
        self.assertEqual(pynamo_model.Meta.table_name, TEST_TABLE)
        self.assertEqual(pynamo_model.Meta.region, 'us-west-2')
        self.assertEqual(pynamo_model.Meta.aws_access_key_id, 'test_key')
        self.assertEqual(pynamo_model.Meta.aws_secret_access_key, 'test_secret')

        # Check entity_id is hash key
        self.assertTrue(hasattr(pynamo_model, 'entity_id'))
        self.assertTrue(pynamo_model.entity_id.is_hash_key)

    @patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}, clear=True)
    def test_generate_model_audit_table(self):
        """
        Test generating PynamoDB model for audit table.

        Verifies:
        - entity_id is hash key
        - version is range key
        - Class name includes 'Audit'
        """
        adapter = DynamoDbAdapter()
        audit_table = f"{TEST_TABLE}_audit"
        pynamo_model = adapter._generate_pynamo_model(audit_table, TestModel, is_audit=True)

        # Check class name
        self.assertEqual(pynamo_model.__name__, "PynamoTestModelAudit")

        # Check keys
        self.assertTrue(pynamo_model.entity_id.is_hash_key)
        self.assertTrue(hasattr(pynamo_model, 'version'))
        self.assertTrue(pynamo_model.version.is_range_key)

    def test_generate_model_maps_dataclass_fields(self):
        """Test that dataclass fields are mapped to PynamoDB attributes."""
        adapter = DynamoDbAdapter()
        pynamo_model = adapter._generate_pynamo_model(TEST_TABLE, TestModel, is_audit=False)

        # Check field mappings
        self.assertTrue(hasattr(pynamo_model, 'name'))
        self.assertTrue(hasattr(pynamo_model, 'age'))
        self.assertTrue(hasattr(pynamo_model, 'active'))
        self.assertTrue(hasattr(pynamo_model, 'metadata'))
        self.assertTrue(hasattr(pynamo_model, 'tags'))


class TestDynamoDbAdapterRunTransaction(unittest.TestCase):
    """Test run_transaction method."""

    def test_run_transaction_executes_callables(self):
        """Test that run_transaction executes all callable operations."""
        adapter = DynamoDbAdapter()

        mock_op1 = Mock()
        mock_op2 = Mock()
        mock_op3 = Mock()

        adapter.run_transaction([mock_op1, mock_op2, mock_op3])

        mock_op1.assert_called_once()
        mock_op2.assert_called_once()
        mock_op3.assert_called_once()

    def test_run_transaction_empty_list(self):
        """Test run_transaction with empty operations list."""
        adapter = DynamoDbAdapter()
        # Should not raise
        adapter.run_transaction([])


class TestDynamoDbAdapterExecuteQuery(unittest.TestCase):
    """Test execute_query method."""

    def test_execute_query_not_implemented(self):
        """Test that execute_query raises NotImplementedError."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(NotImplementedError) as context:
            adapter.execute_query("SELECT * FROM table")

        self.assertIn("not supported", str(context.exception))


class TestDynamoDbAdapterParseResponse(unittest.TestCase):
    """Test parse_db_response method."""

    def test_parse_response_list_of_models(self):
        """Test parsing list of PynamoDB Model instances."""
        adapter = DynamoDbAdapter()

        mock_item1 = Mock(spec=Model)
        mock_item1.attribute_values = {'id': 1, 'name': 'item1'}
        mock_item2 = Mock(spec=Model)
        mock_item2.attribute_values = {'id': 2, 'name': 'item2'}

        result = adapter.parse_db_response([mock_item1, mock_item2])

        self.assertEqual(result, [
            {'id': 1, 'name': 'item1'},
            {'id': 2, 'name': 'item2'}
        ])

    def test_parse_response_single_model(self):
        """Test parsing single PynamoDB Model instance."""
        adapter = DynamoDbAdapter()

        mock_item = Mock(spec=Model)
        mock_item.attribute_values = {'id': 1, 'name': 'item'}

        result = adapter.parse_db_response(mock_item)

        self.assertEqual(result, {'id': 1, 'name': 'item'})

    def test_parse_response_non_model(self):
        """Test parsing non-Model response returns as-is."""
        adapter = DynamoDbAdapter()

        data = {'custom': 'data'}
        result = adapter.parse_db_response(data)

        self.assertEqual(result, data)


class TestDynamoDbAdapterGetOne(unittest.TestCase):
    """Test get_one method."""

    def test_get_one_requires_model_cls(self):
        """Test get_one raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.get_one(TEST_TABLE, {'entity_id': TEST_ENTITY_ID})

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_success(self, mock_generate, mock_execute):
        """Test get_one returns first result."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls

        mock_item = Mock()
        mock_item.attribute_values = {'entity_id': TEST_ENTITY_ID, 'name': 'test'}
        mock_execute.return_value = iter([mock_item])

        result = adapter.get_one(TEST_TABLE, {'entity_id': TEST_ENTITY_ID}, model_cls=TestModel)

        self.assertEqual(result, {'entity_id': TEST_ENTITY_ID, 'name': 'test'})
        mock_generate.assert_called_once_with(TEST_TABLE, TestModel)
        mock_execute.assert_called_once_with(mock_model_cls, {'entity_id': TEST_ENTITY_ID}, limit=1)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_not_found(self, mock_generate, mock_execute):
        """Test get_one returns None when no results."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls
        mock_execute.return_value = iter([])  # Empty iterator

        result = adapter.get_one(TEST_TABLE, {'entity_id': TEST_ENTITY_ID}, model_cls=TestModel)

        self.assertIsNone(result)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_exception(self, mock_generate, mock_execute):
        """Test get_one raises RuntimeError on exception."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls
        mock_execute.side_effect = Exception("DB error")

        with self.assertRaises(RuntimeError) as context:
            adapter.get_one(TEST_TABLE, {'entity_id': TEST_ENTITY_ID}, model_cls=TestModel)

        self.assertIn("get_one failed", str(context.exception))


class TestDynamoDbAdapterGetMany(unittest.TestCase):
    """Test get_many method."""

    def test_get_many_requires_model_cls(self):
        """Test get_many raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.get_many(TEST_TABLE, {})

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_many_success(self, mock_generate, mock_execute):
        """Test get_many returns all results."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls

        mock_item1 = Mock()
        mock_item1.attribute_values = {'id': 1}
        mock_item2 = Mock()
        mock_item2.attribute_values = {'id': 2}
        mock_execute.return_value = [mock_item1, mock_item2]

        result = adapter.get_many(TEST_TABLE, {'active': True}, limit=50, model_cls=TestModel)

        self.assertEqual(result, [{'id': 1}, {'id': 2}])
        mock_execute.assert_called_once_with(mock_model_cls, {'active': True}, limit=50)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_many_exception(self, mock_generate, mock_execute):
        """Test get_many raises RuntimeError on exception."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls
        mock_execute.side_effect = Exception("DB error")

        with self.assertRaises(RuntimeError) as context:
            adapter.get_many(TEST_TABLE, {}, model_cls=TestModel)

        self.assertIn("get_many failed", str(context.exception))


class TestDynamoDbAdapterGetCount(unittest.TestCase):
    """Test get_count method."""

    def test_get_count_requires_model_cls(self):
        """Test get_count raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.get_count(TEST_TABLE, {})

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_count_success(self, mock_generate, mock_execute):
        """Test get_count returns count."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls
        mock_execute.return_value = 42

        result = adapter.get_count(TEST_TABLE, {'active': True}, model_cls=TestModel)

        self.assertEqual(result, 42)
        mock_execute.assert_called_once_with(mock_model_cls, {'active': True}, count_only=True)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_count_exception(self, mock_generate, mock_execute):
        """Test get_count raises RuntimeError on exception."""
        adapter = DynamoDbAdapter()
        mock_model_cls = Mock()
        mock_generate.return_value = mock_model_cls
        mock_execute.side_effect = Exception("DB error")

        with self.assertRaises(RuntimeError) as context:
            adapter.get_count(TEST_TABLE, {}, model_cls=TestModel)

        self.assertIn("get_count failed", str(context.exception))


class TestDynamoDbAdapterMoveToAudit(unittest.TestCase):
    """Test move_entity_to_audit_table methods."""

    def test_get_move_query_returns_lambda(self):
        """Test get_move_entity_to_audit_table_query returns callable."""
        adapter = DynamoDbAdapter()
        query = adapter.get_move_entity_to_audit_table_query(TEST_TABLE, TEST_ENTITY_ID, TestModel)

        self.assertTrue(callable(query))

    def test_move_to_audit_requires_model_cls(self):
        """Test move_entity_to_audit_table raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID)

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_move_to_audit_success(self, mock_generate):
        """Test move_entity_to_audit_table copies item to audit table."""
        adapter = DynamoDbAdapter()

        # Mock standard model
        mock_standard_model = MagicMock()
        mock_item = MagicMock()
        mock_item.attribute_values = {'entity_id': TEST_ENTITY_ID, 'data': 'test'}
        mock_standard_model.get.return_value = mock_item

        # Mock audit model
        mock_audit_model = MagicMock()
        mock_audit_instance = MagicMock()
        mock_audit_model.return_value = mock_audit_instance

        # Return standard model first, then audit model
        mock_generate.side_effect = [mock_standard_model, mock_audit_model]

        adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID, TestModel)

        # Verify standard model get called
        mock_standard_model.get.assert_called_once_with(TEST_ENTITY_ID)

        # Verify audit model created with item data
        mock_audit_model.assert_called_once_with(**mock_item.attribute_values)

        # Verify audit item saved
        mock_audit_instance.save.assert_called_once()

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_move_to_audit_does_not_exist(self, mock_generate):
        """Test move_entity_to_audit_table handles DoesNotExist silently."""
        adapter = DynamoDbAdapter()

        mock_standard_model = MagicMock()
        mock_standard_model.get.side_effect = DoesNotExist()
        mock_generate.return_value = mock_standard_model

        # Should not raise
        adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID, TestModel)

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_move_to_audit_other_exception(self, mock_generate):
        """Test move_entity_to_audit_table raises RuntimeError on other exceptions."""
        adapter = DynamoDbAdapter()

        mock_standard_model = MagicMock()
        mock_standard_model.get.side_effect = Exception("DB error")
        mock_generate.return_value = mock_standard_model

        with self.assertRaises(RuntimeError) as context:
            adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID, TestModel)

        self.assertIn("move_entity_to_audit_table failed", str(context.exception))


class TestDynamoDbAdapterSave(unittest.TestCase):
    """Test save methods."""

    def test_get_save_query_returns_lambda(self):
        """Test get_save_query returns callable."""
        adapter = DynamoDbAdapter()
        query = adapter.get_save_query(TEST_TABLE, {'data': 'test'}, TestModel)

        self.assertTrue(callable(query))

    def test_save_requires_model_cls(self):
        """Test save raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.save(TEST_TABLE, {'data': 'test'})

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_save_success(self, mock_generate):
        """Test save creates and saves item."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_item = MagicMock()
        mock_item.attribute_values = {'entity_id': TEST_ENTITY_ID, 'name': 'test'}
        mock_model_cls.return_value = mock_item
        mock_generate.return_value = mock_model_cls

        data = {'entity_id': TEST_ENTITY_ID, 'name': 'test'}
        result = adapter.save(TEST_TABLE, data, TestModel)

        # Verify model instantiated with data
        mock_model_cls.assert_called_once_with(**data)

        # Verify item saved
        mock_item.save.assert_called_once()

        # Verify return value
        self.assertEqual(result, {'entity_id': TEST_ENTITY_ID, 'name': 'test'})


class TestDynamoDbAdapterDelete(unittest.TestCase):
    """Test delete method."""

    def test_delete_requires_model_cls(self):
        """Test delete raises ValueError without model_cls."""
        adapter = DynamoDbAdapter()
        with self.assertRaises(ValueError) as context:
            adapter.delete(TEST_TABLE, {'entity_id': TEST_ENTITY_ID})

        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_delete_success(self, mock_generate):
        """Test delete sets active=False and saves."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_item = MagicMock()
        mock_item.active = True
        mock_model_cls.get.return_value = mock_item
        mock_generate.return_value = mock_model_cls

        result = adapter.delete(TEST_TABLE, {'entity_id': TEST_ENTITY_ID}, TestModel)

        # Verify item retrieved
        mock_model_cls.get.assert_called_once_with(TEST_ENTITY_ID)

        # Verify active set to False
        self.assertFalse(mock_item.active)

        # Verify item saved
        mock_item.save.assert_called_once()

        # Verify return value
        self.assertTrue(result)

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_delete_does_not_exist(self, mock_generate):
        """Test delete returns False when item does not exist."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_model_cls.get.side_effect = DoesNotExist()
        mock_generate.return_value = mock_model_cls

        result = adapter.delete(TEST_TABLE, {'entity_id': TEST_ENTITY_ID}, TestModel)

        self.assertFalse(result)

    def test_delete_no_entity_id(self):
        """Test delete returns False when entity_id not in data."""
        adapter = DynamoDbAdapter()

        result = adapter.delete(TEST_TABLE, {'name': 'test'}, TestModel)

        self.assertFalse(result)


class TestDynamoDbAdapterGetModelKeys(unittest.TestCase):
    """Test _get_model_keys helper method."""

    def test_get_model_keys_hash_only(self):
        """Test retrieving hash key only."""
        adapter = DynamoDbAdapter()

        # Create mock model with hash key
        mock_model = MagicMock()
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False

        mock_model.get_attributes.return_value = {
            'entity_id': mock_hash_attr
        }

        hash_key, range_key = adapter._get_model_keys(mock_model)

        self.assertEqual(hash_key, 'entity_id')
        self.assertIsNone(range_key)

    def test_get_model_keys_hash_and_range(self):
        """Test retrieving both hash and range keys."""
        adapter = DynamoDbAdapter()

        # Create mock model with both keys
        mock_model = MagicMock()
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False

        mock_range_attr = MagicMock()
        mock_range_attr.is_hash_key = False
        mock_range_attr.is_range_key = True

        mock_model.get_attributes.return_value = {
            'entity_id': mock_hash_attr,
            'version': mock_range_attr
        }

        hash_key, range_key = adapter._get_model_keys(mock_model)

        self.assertEqual(hash_key, 'entity_id')
        self.assertEqual(range_key, 'version')


class TestDynamoDbAdapterExecuteQuery(unittest.TestCase):
    """Test _execute_query helper method."""

    def test_execute_query_hash_key_only(self):
        """Test query with only hash key condition."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_model_cls.query.return_value = ['result1', 'result2']

        conditions = {'entity_id': TEST_ENTITY_ID}
        _result = adapter._execute_query(
            mock_model_cls, conditions, 'entity_id', None, TEST_ENTITY_ID, limit=10
        )

        mock_model_cls.query.assert_called_once_with(
            TEST_ENTITY_ID,
            range_key_condition=None,
            filter_condition=None,
            limit=10
        )

    def test_execute_query_count_only(self):
        """Test query with count_only=True."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_model_cls.count.return_value = 42

        conditions = {'entity_id': TEST_ENTITY_ID}
        result = adapter._execute_query(
            mock_model_cls, conditions, 'entity_id', None, TEST_ENTITY_ID, count_only=True
        )

        self.assertEqual(result, 42)
        mock_model_cls.count.assert_called_once()


class TestDynamoDbAdapterExecuteScan(unittest.TestCase):
    """Test _execute_scan helper method."""

    def test_execute_scan_with_conditions(self):
        """Test scan with filter conditions."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_attr = MagicMock()
        mock_condition = MagicMock()
        mock_attr.__eq__ = Mock(return_value=mock_condition)
        mock_model_cls.active = mock_attr

        mock_model_cls.scan.return_value = ['result1', 'result2']

        conditions = {'active': True}
        _result = adapter._execute_scan(mock_model_cls, conditions, limit=50)

        # Verify scan was called with condition
        self.assertEqual(mock_model_cls.scan.call_count, 1)

    def test_execute_scan_count_only(self):
        """Test scan with count_only=True."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_model_cls.count.return_value = 100

        result = adapter._execute_scan(mock_model_cls, None, count_only=True)

        self.assertEqual(result, 100)
        mock_model_cls.count.assert_called_once_with(filter_condition=None)

    def test_execute_scan_no_conditions(self):
        """Test scan without any filter conditions."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_model_cls.scan.return_value = ['result']

        _result = adapter._execute_scan(mock_model_cls, None, limit=10)

        mock_model_cls.scan.assert_called_once_with(None, limit=10)


class TestDynamoDbAdapterExecuteQueryOrScan(unittest.TestCase):
    """Test _execute_query_or_scan dispatcher method."""

    @patch.object(DynamoDbAdapter, '_get_model_keys')
    @patch.object(DynamoDbAdapter, '_execute_query')
    def test_uses_query_when_hash_key_present(self, mock_execute_query, mock_get_keys):
        """Test that Query is used when hash key value is provided."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_get_keys.return_value = ('entity_id', None)
        mock_execute_query.return_value = ['result']

        conditions = {'entity_id': TEST_ENTITY_ID, 'active': True}
        _result = adapter._execute_query_or_scan(mock_model_cls, conditions)

        # Verify Query was called, not Scan
        mock_execute_query.assert_called_once_with(
            mock_model_cls, conditions, 'entity_id', None, TEST_ENTITY_ID, None, False
        )

    @patch.object(DynamoDbAdapter, '_get_model_keys')
    @patch.object(DynamoDbAdapter, '_execute_scan')
    def test_uses_scan_when_hash_key_absent(self, mock_execute_scan, mock_get_keys):
        """Test that Scan is used when hash key value is not provided."""
        adapter = DynamoDbAdapter()

        mock_model_cls = MagicMock()
        mock_get_keys.return_value = ('entity_id', None)
        mock_execute_scan.return_value = ['result']

        conditions = {'active': True}  # No entity_id
        _result = adapter._execute_query_or_scan(mock_model_cls, conditions)

        # Verify Scan was called, not Query
        mock_execute_scan.assert_called_once_with(mock_model_cls, conditions, None, False)


if __name__ == '__main__':
    unittest.main()
