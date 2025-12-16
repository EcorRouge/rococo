import unittest
import os
from uuid import uuid4
from dataclasses import dataclass
from unittest.mock import MagicMock, patch, ANY
from typing import Type, Optional
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, BooleanAttribute
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.repositories.dynamodb.dynamodb_repository import DynamoDbRepository
from rococo.models.versioned_model import VersionedModel
from rococo.messaging import MessageAdapter

# Dummy PynamoDB Models
class PersonModel(Model):
    class Meta:
        table_name = 'person'
        region = 'us-east-1'
    entity_id = UnicodeAttribute(hash_key=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    active = BooleanAttribute(default=True)
    version = UnicodeAttribute(null=True)
    previous_version = UnicodeAttribute(null=True)
    changed_by_id = UnicodeAttribute(null=True)
    changed_on = UnicodeAttribute(null=True)
    attribute_values = {} 

    def __init__(self, **kwargs):
        # super().__init__(**kwargs)
        self.attribute_values = kwargs

    def save(self):
        # Stub for testing - actual save is mocked
        pass
    
    def delete(self):
        # Stub for testing - actual delete is mocked
        pass

class PersonAuditModel(Model):
    class Meta:
        table_name = 'person_audit'
        region = 'us-east-1'
    entity_id = UnicodeAttribute(hash_key=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    active = BooleanAttribute(default=True)
    version = UnicodeAttribute(null=True)
    previous_version = UnicodeAttribute(null=True)
    changed_by_id = UnicodeAttribute(null=True)
    changed_on = UnicodeAttribute(null=True)
    attribute_values = {}

    def __init__(self, **kwargs):
        # super().__init__(**kwargs)
        self.attribute_values = kwargs

    def save(self):
        pass

# Dummy Rococo Model
@dataclass
class Person(VersionedModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None

class TestDynamoDbRepository(unittest.TestCase):
    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        self.adapter = DynamoDbAdapter()
        self.message_adapter = MagicMock(spec=MessageAdapter)
        self.repository = DynamoDbRepository(
            self.adapter,
            Person,
            self.message_adapter,
            'test_queue'
        )

        # Patch _generate_pynamo_model to return our dummy models
        self.patcher = patch.object(self.adapter, '_generate_pynamo_model', side_effect=self._mock_generate_model)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def _mock_generate_model(self, table_name, model_cls, is_audit=False):
        if is_audit:
            return PersonAuditModel
        return PersonModel

    def test_get_one_query(self):
        # Test get_one using query (hash key present)
        # Mock get_attributes for _execute_query_or_scan
        PersonModel.get_attributes = MagicMock()
        
        # Mock attributes to identify hash key
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        entity_id = uuid4().hex

        with patch.object(PersonModel, 'query') as mock_query:
            mock_item = MagicMock()
            mock_item.attribute_values = {'entity_id': entity_id, 'first_name': 'John', 'active': True}
            mock_query.return_value = [mock_item]

            result = self.repository.get_one({'entity_id': entity_id})
            
            self.assertIsNotNone(result)
            self.assertEqual(result.entity_id, entity_id)
            self.assertEqual(result.first_name, 'John')
            mock_query.assert_called_once()

    def test_get_one_scan(self):
        # Test get_one using scan (hash key missing)
        # Mock get_attributes for _execute_query_or_scan
        PersonModel.get_attributes = MagicMock()
        
        # Mock attributes to identify hash key
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        entity_id = uuid4().hex

        with patch.object(PersonModel, 'scan') as mock_scan:
            mock_item = MagicMock()
            mock_item.attribute_values = {'entity_id': entity_id, 'first_name': 'John', 'active': True}
            mock_scan.return_value = [mock_item]

            result = self.repository.get_one({'first_name': 'John'})
            
            self.assertIsNotNone(result)
            self.assertEqual(result.first_name, 'John')
            mock_scan.assert_called_once()

    def test_save_new(self):
        # Test saving a new record
        person = Person(first_name='Jane', last_name='Doe')
        
        with patch.object(PersonModel, 'save') as mock_save:
            saved_person = self.repository.save(person, send_message=True)
            
            self.assertEqual(saved_person.first_name, 'Jane')
            mock_save.assert_called()
            
            # Verify message was sent
            self.message_adapter.send_message.assert_called_with(
                'test_queue', 
                ANY  # The message body (JSON)
            )

    def test_save_existing_audit(self):
        # Test saving an existing record (should trigger audit)
        person = Person(first_name='Jane', last_name='Doe')
        entity_id = uuid4().hex
        old_version = uuid4().hex
        person.entity_id = entity_id
        person.previous_version = old_version
        
        # Mock the 'get' call used by move_entity_to_audit_table
        with patch.object(PersonModel, 'get') as mock_get:
            mock_item = MagicMock()
            mock_item.attribute_values = {'entity_id': entity_id, 'first_name': 'Jane', 'active': True}
            mock_get.return_value = mock_item
            
            with patch.object(PersonAuditModel, 'save') as mock_audit_save:
                with patch.object(PersonModel, 'save') as mock_save:
                    self.repository.save(person, send_message=True)
                    
                    # Ensure we tried to fetch the old record to audit it
                    mock_get.assert_called_with(entity_id)
                    
                    # Ensure the audit record was saved
                    mock_audit_save.assert_called()
                    
                    # Ensure the new record was saved
                    mock_save.assert_called()
                    
                    # Verify message was sent
                    self.message_adapter.send_message.assert_called()

    def test_delete(self):
        person = Person(first_name='Jane')
        person.entity_id = uuid4().hex
        
        # Mock get for the delete check (if repository does a check) or just the save (soft delete)
        with patch.object(PersonModel, 'save') as mock_save:
             self.repository.delete(person)
             mock_save.assert_called()

    def test_get_one_not_found(self):
        """Test get_one returns None when record not found."""
        PersonModel.get_attributes = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        with patch.object(PersonModel, 'query') as mock_query:
            mock_query.return_value = []
            
            result = self.repository.get_one({'entity_id': uuid4().hex})
            
            self.assertIsNone(result)

    def test_get_many_empty_results(self):
        """Test get_many returns empty list when no records found."""
        PersonModel.get_attributes = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        with patch.object(PersonModel, 'scan') as mock_scan:
            mock_scan.return_value = []
            
            result = self.repository.get_many({'first_name': 'NonExistent'})
            
            self.assertEqual(result, [])

    def test_get_many_multiple_records(self):
        """Test get_many returns multiple records."""
        PersonModel.get_attributes = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        with patch.object(PersonModel, 'scan') as mock_scan:
            mock_item1 = MagicMock()
            mock_item1.attribute_values = {'entity_id': uuid4().hex, 'first_name': 'John', 'active': True}
            mock_item2 = MagicMock()
            mock_item2.attribute_values = {'entity_id': uuid4().hex, 'first_name': 'Jane', 'active': True}
            mock_scan.return_value = [mock_item1, mock_item2]
            
            result = self.repository.get_many({'active': True})
            
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0].first_name, 'John')
            self.assertEqual(result[1].first_name, 'Jane')

    def test_get_many_with_no_conditions(self):
        """Test get_many with no conditions (scans all)."""
        PersonModel.get_attributes = MagicMock()
        
        mock_hash_attr = MagicMock()
        mock_hash_attr.is_hash_key = True
        mock_hash_attr.is_range_key = False
        
        PersonModel.get_attributes.return_value = {'entity_id': mock_hash_attr}
        
        with patch.object(PersonModel, 'scan') as mock_scan:
            mock_item = MagicMock()
            mock_item.attribute_values = {'entity_id': uuid4().hex, 'first_name': 'John', 'active': True}
            mock_scan.return_value = [mock_item]
            
            result = self.repository.get_many()
            
            self.assertEqual(len(result), 1)

    def test_save_without_message(self):
        """Test saving a record without sending message."""
        person = Person(first_name='Jane', last_name='Doe')
        
        with patch.object(PersonModel, 'save') as mock_save:
            saved_person = self.repository.save(person, send_message=False)
            
            self.assertEqual(saved_person.first_name, 'Jane')
            mock_save.assert_called()
            
            # Verify message was NOT sent
            self.message_adapter.send_message.assert_not_called()

    def test_delete_sets_active_false(self):
        """Test delete sets active=False on the instance."""
        person = Person(first_name='Jane')
        entity_id = uuid4().hex
        person.entity_id = entity_id
        
        with patch.object(PersonModel, 'save') as mock_save:
            deleted_person = self.repository.delete(person)
            
            # Ensure active is set to False
            self.assertFalse(deleted_person.active)
            
            # Ensure save was called
            mock_save.assert_called()

    def test_process_data_before_save(self):
        """Test _process_data_before_save prepares data correctly."""
        person = Person(first_name='Jane', last_name='Doe')
        
        data = self.repository._process_data_before_save(person)
        
        self.assertIn('entity_id', data)
        self.assertIn('first_name', data)
        self.assertEqual(data['first_name'], 'Jane')
        # Check that datetime is converted to ISO string
        self.assertIn('changed_on', data)
        self.assertIsInstance(data['changed_on'], str)


if __name__ == '__main__':
    unittest.main()

