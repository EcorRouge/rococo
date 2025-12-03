import unittest
import os
from uuid import uuid4
from dataclasses import dataclass
from unittest.mock import MagicMock, patch, ANY
from typing import Type
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
        pass
    
    def delete(self):
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
    first_name: str = None
    last_name: str = None

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


if __name__ == '__main__':
    unittest.main()
