"""
Comprehensive unit tests for rococo/data/dynamodb.py
"""
import unittest
import os
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4

from rococo.data.dynamodb import DynamoDbAdapter
from rococo.models.versioned_model import VersionedModel


@dataclass
class SampleModel(VersionedModel):
    """Sample model for DynamoDB adapter tests."""
    name: Optional[str] = None
    age: Optional[int] = None
    score: Optional[float] = None
    active_flag: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None


class TestDynamoDbAdapterTypeMapping(unittest.TestCase):
    """Tests for _map_type_to_attribute method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_REGION'] = 'us-east-1'
        self.adapter = DynamoDbAdapter()

    def test_map_bool_type(self):
        """Test mapping bool type to BooleanAttribute."""
        attr = self.adapter._map_type_to_attribute(bool)
        self.assertEqual(attr.__class__.__name__, 'BooleanAttribute')
        self.assertTrue(attr.null)

    def test_map_int_type(self):
        """Test mapping int type to NumberAttribute."""
        attr = self.adapter._map_type_to_attribute(int)
        self.assertEqual(attr.__class__.__name__, 'NumberAttribute')

    def test_map_float_type(self):
        """Test mapping float type to NumberAttribute."""
        attr = self.adapter._map_type_to_attribute(float)
        self.assertEqual(attr.__class__.__name__, 'NumberAttribute')

    def test_map_dict_type(self):
        """Test mapping dict type to JSONAttribute."""
        attr = self.adapter._map_type_to_attribute(dict)
        self.assertEqual(attr.__class__.__name__, 'JSONAttribute')

    def test_map_list_type(self):
        """Test mapping list type to ListAttribute."""
        attr = self.adapter._map_type_to_attribute(list)
        self.assertEqual(attr.__class__.__name__, 'ListAttribute')

    def test_map_str_type(self):
        """Test mapping str type to UnicodeAttribute (default)."""
        attr = self.adapter._map_type_to_attribute(str)
        self.assertEqual(attr.__class__.__name__, 'UnicodeAttribute')

    def test_map_unknown_type_defaults_to_unicode(self):
        """Test that unknown types default to UnicodeAttribute."""
        attr = self.adapter._map_type_to_attribute(complex)
        self.assertEqual(attr.__class__.__name__, 'UnicodeAttribute')

    def test_map_with_hash_key(self):
        """Test mapping with hash_key=True."""
        attr = self.adapter._map_type_to_attribute(str, is_hash_key=True)
        self.assertTrue(attr.is_hash_key)

    def test_map_with_range_key(self):
        """Test mapping with range_key=True."""
        attr = self.adapter._map_type_to_attribute(str, is_range_key=True)
        self.assertTrue(attr.is_range_key)


class TestDynamoDbAdapterContextManager(unittest.TestCase):
    """Tests for context manager methods."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_enter_returns_self(self):
        """Test __enter__ returns the adapter instance."""
        result = self.adapter.__enter__()
        self.assertIs(result, self.adapter)

    def test_exit_does_nothing(self):
        """Test __exit__ executes without error."""
        # Should not raise any exceptions
        self.adapter.__exit__(None, None, None)

    def test_context_manager_usage(self):
        """Test using adapter as context manager."""
        with DynamoDbAdapter() as adapter:
            self.assertIsInstance(adapter, DynamoDbAdapter)


class TestDynamoDbAdapterGeneratePynamoModel(unittest.TestCase):
    """Tests for _generate_pynamo_model method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_REGION'] = 'us-east-1'
        self.adapter = DynamoDbAdapter()

    def test_generate_model_for_regular_table(self):
        """Test generating a PynamoDB model for a regular table."""
        pynamo_model = self.adapter._generate_pynamo_model('test_table', SampleModel)
        
        # Check class name
        self.assertEqual(pynamo_model.__name__, 'PynamoSampleModel')
        
        # Check Meta attributes
        self.assertEqual(pynamo_model.Meta.table_name, 'test_table')
        self.assertEqual(pynamo_model.Meta.region, 'us-east-1')

    def test_generate_model_for_audit_table(self):
        """Test generating a PynamoDB model for an audit table."""
        pynamo_model = self.adapter._generate_pynamo_model('test_table_audit', SampleModel, is_audit=True)
        
        # Check class name includes Audit
        self.assertEqual(pynamo_model.__name__, 'PynamoSampleModelAudit')
        
        # Audit tables should have entity_id as hash key and version as range key
        attrs = pynamo_model._attributes if hasattr(pynamo_model, '_attributes') else {}
        # The model should have been created with proper keys

    def test_generate_model_includes_dataclass_fields(self):
        """Test that generated model includes fields from dataclass."""
        pynamo_model = self.adapter._generate_pynamo_model('test_table', SampleModel)
        
        # Check that custom fields are present
        self.assertTrue(hasattr(pynamo_model, 'name'))
        self.assertTrue(hasattr(pynamo_model, 'age'))
        self.assertTrue(hasattr(pynamo_model, 'entity_id'))


class TestDynamoDbAdapterRunTransaction(unittest.TestCase):
    """Tests for run_transaction method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_run_transaction_executes_callables(self):
        """Test that run_transaction executes all callable operations."""
        mock_op1 = MagicMock()
        mock_op2 = MagicMock()
        
        self.adapter.run_transaction([mock_op1, mock_op2])
        
        mock_op1.assert_called_once()
        mock_op2.assert_called_once()

    def test_run_transaction_ignores_non_callables(self):
        """Test that run_transaction ignores non-callable items."""
        mock_op = MagicMock()
        
        # Should not raise an error for non-callables
        self.adapter.run_transaction([mock_op, "not_callable", 123])
        
        mock_op.assert_called_once()

    def test_run_transaction_empty_list(self):
        """Test run_transaction with empty list."""
        # Should not raise any exceptions
        self.adapter.run_transaction([])


class TestDynamoDbAdapterExecuteQuery(unittest.TestCase):
    """Tests for execute_query method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_execute_query_raises_not_implemented(self):
        """Test that execute_query raises NotImplementedError."""
        with self.assertRaises(NotImplementedError) as context:
            self.adapter.execute_query("SELECT * FROM table")
        
        self.assertIn("not supported for DynamoDB", str(context.exception))


class TestDynamoDbAdapterParseDbResponse(unittest.TestCase):
    """Tests for parse_db_response method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_parse_list_response(self):
        """Test parsing a list of items."""
        mock_item1 = MagicMock()
        mock_item1.attribute_values = {'id': '1', 'name': 'Item 1'}
        mock_item2 = MagicMock()
        mock_item2.attribute_values = {'id': '2', 'name': 'Item 2'}
        
        result = self.adapter.parse_db_response([mock_item1, mock_item2])
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], '1')
        self.assertEqual(result[1]['id'], '2')

    def test_parse_model_response(self):
        """Test parsing a single Model response."""
        from pynamodb.models import Model
        
        mock_model = MagicMock(spec=Model)
        mock_model.attribute_values = {'id': '1', 'name': 'Test'}
        
        result = self.adapter.parse_db_response(mock_model)
        
        self.assertEqual(result['id'], '1')
        self.assertEqual(result['name'], 'Test')

    def test_parse_other_response(self):
        """Test parsing non-list, non-Model response returns as-is."""
        response = {'raw': 'data'}
        
        result = self.adapter.parse_db_response(response)
        
        self.assertEqual(result, response)


class TestDynamoDbAdapterGetOne(unittest.TestCase):
    """Tests for get_one method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_get_one_requires_model_cls(self):
        """Test that get_one requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.get_one('table', {'id': '1'})
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_returns_first_item(self, mock_generate, mock_execute):
        """Test get_one returns the first matching item."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        
        mock_item = MagicMock()
        mock_item.attribute_values = {'entity_id': '123', 'name': 'Test'}
        mock_execute.return_value = [mock_item]
        
        result = self.adapter.get_one('table', {'entity_id': '123'}, model_cls=SampleModel)
        
        self.assertEqual(result['entity_id'], '123')

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_returns_none_when_not_found(self, mock_generate, mock_execute):
        """Test get_one returns None when no item found."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        mock_execute.return_value = []
        
        result = self.adapter.get_one('table', {'entity_id': '123'}, model_cls=SampleModel)
        
        self.assertIsNone(result)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_one_raises_on_exception(self, mock_generate, mock_execute):
        """Test get_one raises RuntimeError on exception."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        mock_execute.side_effect = Exception("DynamoDB error")
        
        with self.assertRaises(RuntimeError) as context:
            self.adapter.get_one('table', {'entity_id': '123'}, model_cls=SampleModel)
        
        self.assertIn("get_one failed", str(context.exception))


class TestDynamoDbAdapterGetMany(unittest.TestCase):
    """Tests for get_many method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_get_many_requires_model_cls(self):
        """Test that get_many requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.get_many('table', {'id': '1'})
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_many_returns_all_items(self, mock_generate, mock_execute):
        """Test get_many returns all matching items."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        
        mock_item1 = MagicMock()
        mock_item1.attribute_values = {'entity_id': '1', 'name': 'Test1'}
        mock_item2 = MagicMock()
        mock_item2.attribute_values = {'entity_id': '2', 'name': 'Test2'}
        mock_execute.return_value = [mock_item1, mock_item2]
        
        result = self.adapter.get_many('table', {'active': True}, model_cls=SampleModel)
        
        self.assertEqual(len(result), 2)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_many_raises_on_exception(self, mock_generate, mock_execute):
        """Test get_many raises RuntimeError on exception."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        mock_execute.side_effect = Exception("DynamoDB error")
        
        with self.assertRaises(RuntimeError) as context:
            self.adapter.get_many('table', model_cls=SampleModel)
        
        self.assertIn("get_many failed", str(context.exception))


class TestDynamoDbAdapterGetCount(unittest.TestCase):
    """Tests for get_count method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_get_count_requires_model_cls(self):
        """Test that get_count requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.get_count('table', {'id': '1'})
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_count_returns_count(self, mock_generate, mock_execute):
        """Test get_count returns the count value."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        mock_execute.return_value = 5
        
        result = self.adapter.get_count('table', {'active': True}, model_cls=SampleModel)
        
        self.assertEqual(result, 5)
        mock_execute.assert_called_with(mock_model, {'active': True}, count_only=True)

    @patch.object(DynamoDbAdapter, '_execute_query_or_scan')
    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_get_count_raises_on_exception(self, mock_generate, mock_execute):
        """Test get_count raises RuntimeError on exception."""
        mock_model = MagicMock()
        mock_generate.return_value = mock_model
        mock_execute.side_effect = Exception("DynamoDB error")
        
        with self.assertRaises(RuntimeError) as context:
            self.adapter.get_count('table', {'active': True}, model_cls=SampleModel)
        
        self.assertIn("get_count failed", str(context.exception))


class TestDynamoDbAdapterSave(unittest.TestCase):
    """Tests for save method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_save_requires_model_cls(self):
        """Test that save requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.save('table', {'name': 'Test'})
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_save_creates_and_saves_item(self, mock_generate):
        """Test save creates a PynamoDB item and saves it."""
        mock_model_class = MagicMock()
        mock_item = MagicMock()
        mock_item.attribute_values = {'entity_id': '123', 'name': 'Test'}
        mock_model_class.return_value = mock_item
        mock_generate.return_value = mock_model_class
        
        result = self.adapter.save('table', {'entity_id': '123', 'name': 'Test'}, model_cls=SampleModel)
        
        mock_item.save.assert_called_once()
        self.assertEqual(result['entity_id'], '123')


class TestDynamoDbAdapterDelete(unittest.TestCase):
    """Tests for delete method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_delete_requires_model_cls(self):
        """Test that delete requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.delete('table', {'entity_id': '123'})
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_delete_soft_deletes_item(self, mock_generate):
        """Test delete performs a soft delete (sets active=False)."""
        from pynamodb.exceptions import DoesNotExist
        
        mock_model_class = MagicMock()
        mock_item = MagicMock()
        mock_item.active = True
        mock_model_class.get.return_value = mock_item
        mock_generate.return_value = mock_model_class
        
        result = self.adapter.delete('table', {'entity_id': '123'}, model_cls=SampleModel)
        
        self.assertTrue(result)
        self.assertFalse(mock_item.active)
        mock_item.save.assert_called_once()

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_delete_returns_false_when_not_found(self, mock_generate):
        """Test delete returns False when item doesn't exist."""
        from pynamodb.exceptions import DoesNotExist
        
        mock_model_class = MagicMock()
        mock_model_class.get.side_effect = DoesNotExist()
        mock_generate.return_value = mock_model_class
        
        result = self.adapter.delete('table', {'entity_id': '123'}, model_cls=SampleModel)
        
        self.assertFalse(result)

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_delete_returns_false_without_entity_id(self, mock_generate):
        """Test delete returns False when entity_id is not provided."""
        mock_model_class = MagicMock()
        mock_generate.return_value = mock_model_class
        
        result = self.adapter.delete('table', {'name': 'Test'}, model_cls=SampleModel)
        
        self.assertFalse(result)


class TestDynamoDbAdapterMoveToAudit(unittest.TestCase):
    """Tests for move_entity_to_audit_table method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_move_requires_model_cls(self):
        """Test that move_entity_to_audit_table requires model_cls parameter."""
        with self.assertRaises(ValueError) as context:
            self.adapter.move_entity_to_audit_table('table', '123')
        
        self.assertIn("model_cls is required", str(context.exception))

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_move_copies_to_audit_table(self, mock_generate):
        """Test move_entity_to_audit_table copies item to audit table."""
        mock_main_model = MagicMock()
        mock_audit_model = MagicMock()
        
        mock_item = MagicMock()
        mock_item.attribute_values = {'entity_id': '123', 'name': 'Test'}
        mock_main_model.get.return_value = mock_item
        
        mock_audit_item = MagicMock()
        mock_audit_model.return_value = mock_audit_item
        
        def generate_side_effect(table_name, model_cls, is_audit=False):
            if is_audit:
                return mock_audit_model
            return mock_main_model
        
        mock_generate.side_effect = generate_side_effect
        
        self.adapter.move_entity_to_audit_table('table', '123', model_cls=SampleModel)
        
        mock_main_model.get.assert_called_once_with('123')
        mock_audit_item.save.assert_called_once()

    @patch.object(DynamoDbAdapter, '_generate_pynamo_model')
    def test_move_handles_does_not_exist(self, mock_generate):
        """Test move_entity_to_audit_table handles DoesNotExist gracefully."""
        from pynamodb.exceptions import DoesNotExist
        
        mock_main_model = MagicMock()
        mock_main_model.get.side_effect = DoesNotExist()
        
        mock_generate.return_value = mock_main_model
        
        # Should not raise an exception
        self.adapter.move_entity_to_audit_table('table', '123', model_cls=SampleModel)


class TestDynamoDbAdapterGetQueryMethods(unittest.TestCase):
    """Tests for get_save_query and get_move_entity_to_audit_table_query methods."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_get_save_query_returns_callable(self):
        """Test get_save_query returns a callable lambda."""
        query = self.adapter.get_save_query('table', {'name': 'Test'}, model_cls=SampleModel)
        self.assertTrue(callable(query))

    def test_get_move_entity_to_audit_table_query_returns_callable(self):
        """Test get_move_entity_to_audit_table_query returns a callable lambda."""
        query = self.adapter.get_move_entity_to_audit_table_query('table', '123', model_cls=SampleModel)
        self.assertTrue(callable(query))


class TestDynamoDbAdapterExecuteQueryOrScan(unittest.TestCase):
    """Tests for _execute_query_or_scan method."""

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()

    def test_uses_query_when_hash_key_present(self):
        """Test that query is used when hash key is in conditions."""
        mock_model = MagicMock()
        
        # Setup hash key attribute
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {'entity_id': mock_hash_attr}
        mock_model.query.return_value = []
        
        self.adapter._execute_query_or_scan(mock_model, {'entity_id': '123'})
        
        mock_model.query.assert_called()

    def test_uses_scan_when_hash_key_missing(self):
        """Test that scan is used when hash key is not in conditions."""
        mock_model = MagicMock()
        
        # Setup hash key attribute
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {'entity_id': mock_hash_attr}
        mock_model.scan.return_value = []
        
        self.adapter._execute_query_or_scan(mock_model, {'name': 'Test'})
        
        mock_model.scan.assert_called()

    def test_count_only_with_query(self):
        """Test count_only=True uses model.count with query."""
        mock_model = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {'entity_id': mock_hash_attr}
        mock_model.count.return_value = 5
        
        result = self.adapter._execute_query_or_scan(
            mock_model, {'entity_id': '123'}, count_only=True
        )
        
        self.assertEqual(result, 5)
        mock_model.count.assert_called()

    def test_count_only_with_scan(self):
        """Test count_only=True uses model.count without hash key."""
        mock_model = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {'entity_id': mock_hash_attr}
        mock_model.count.return_value = 10
        
        result = self.adapter._execute_query_or_scan(
            mock_model, {'name': 'Test'}, count_only=True
        )
        
        self.assertEqual(result, 10)

    def test_handles_range_key_condition(self):
        """Test that range key conditions are handled properly."""
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
        mock_model.query.return_value = []
        
        self.adapter._execute_query_or_scan(
            mock_model, {'entity_id': '123', 'version': 'v1'}
        )
        
        mock_model.query.assert_called()

    def test_handles_filter_conditions(self):
        """Test that filter conditions are applied properly."""
        mock_model = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_name_attr = MagicMock()
        mock_name_attr.is_hash_key = False
        mock_name_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {
            'entity_id': mock_hash_attr,
            'name': mock_name_attr
        }
        
        # Mock the attribute access for filter condition building
        mock_model.name = mock_name_attr
        mock_name_attr.__eq__ = MagicMock(return_value=MagicMock())
        
        mock_model.query.return_value = []
        
        self.adapter._execute_query_or_scan(
            mock_model, {'entity_id': '123', 'name': 'Test'}
        )
        
        mock_model.query.assert_called()

    def test_handles_empty_conditions(self):
        """Test handling of empty/None conditions."""
        mock_model = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        mock_model.get_attributes.return_value = {'entity_id': mock_hash_attr}
        mock_model.scan.return_value = []
        
        self.adapter._execute_query_or_scan(mock_model, None)
        
        mock_model.scan.assert_called()


if __name__ == '__main__':
    unittest.main()
