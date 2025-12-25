from typing import Any, Dict, List, Optional, Tuple, Type, Union
import os
from pynamodb.models import Model
from pynamodb.connection import Connection
from pynamodb.transactions import TransactWrite
from pynamodb.attributes import UnicodeAttribute, BooleanAttribute, NumberAttribute, JSONAttribute, UTCDateTimeAttribute, ListAttribute
from pynamodb.exceptions import DoesNotExist
from rococo.data.base import DbAdapter
from rococo.models import BaseModel, VersionedModel


class DynamoOperation:
    def __init__(self, action: str, model: Model, condition=None):
        self.action = action
        self.model = model
        self.condition = condition


class DynamoDbAdapter(DbAdapter):
    """DynamoDB adapter using PynamoDB with dynamic model generation."""

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _map_type_to_attribute(self, field_type: Any, is_hash_key: bool = False, is_range_key: bool = False):
        kwargs = {
            'hash_key': is_hash_key,
            'range_key': is_range_key,
            'null': True
        }
        
        if field_type == bool:
            return BooleanAttribute(**kwargs)
        elif field_type == int or field_type == float:
            return NumberAttribute(**kwargs)
        elif field_type == dict:
            return JSONAttribute(**kwargs)
        elif field_type == list:
            return ListAttribute(**kwargs)
        # Default to UnicodeAttribute for str and others
        return UnicodeAttribute(**kwargs)

    def _generate_pynamo_model(self, table_name: str, model_cls: Type[BaseModel], is_audit: bool = False) -> Type[Model]:
        """Dynamically generate a PynamoDB Model class from a Rococo BaseModel or VersionedModel."""
        
        # 1. Define Meta
        class Meta:
            table_name_val = table_name
            region = os.getenv('AWS_REGION', 'us-east-1')
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        attrs = {
            'Meta': type('Meta', (), {
                'table_name': Meta.table_name_val,
                'region': Meta.region,
                'aws_access_key_id': Meta.aws_access_key_id,
                'aws_secret_access_key': Meta.aws_secret_access_key
            })
        }

        # 2. Map fields
        # If it's an audit table, we might want a different key structure
        # Standard Rococo Audit: entity_id (Hash), version (Range)
        
        if is_audit:
            attrs['entity_id'] = UnicodeAttribute(hash_key=True)
            attrs['version'] = UnicodeAttribute(range_key=True)
        else:
            # Standard Table: entity_id (Hash)
            attrs['entity_id'] = UnicodeAttribute(hash_key=True)

        # Add other fields from dataclass
        if hasattr(model_cls, '__dataclass_fields__'):
            for field_name, field_def in model_cls.__dataclass_fields__.items():
                if field_name == 'entity_id':
                    continue
                if is_audit and field_name == 'version':
                    continue
                
                attrs[field_name] = self._map_type_to_attribute(field_def.type)

        # 3. Create class
        class_name = f"Pynamo{model_cls.__name__}{'Audit' if is_audit else ''}"
        return type(class_name, (Model,), attrs)

    def run_transaction(self, operations_list: List[Any]):
        """Execute operations as a single DynamoDB ACID transaction.

        This adapter intentionally requires `DynamoOperation` entries and will
        raise if anything else is provided.
        """
        ops = [op for op in (operations_list or []) if op is not None]
        if not ops:
            return

        first_op = ops[0]
        if not isinstance(first_op, DynamoOperation):
            raise RuntimeError("DynamoDbAdapter.run_transaction expects DynamoOperation objects")

        region = getattr(getattr(first_op.model, "Meta", None), "region", None) or os.getenv(
            "AWS_REGION", "us-east-1"
        )
        connection = Connection(region=region)

        try:
            with TransactWrite(connection=connection) as transaction:
                for op in ops:
                    if not isinstance(op, DynamoOperation):
                        raise RuntimeError("DynamoDbAdapter.run_transaction expects only DynamoOperation objects")
                    if op.action == "save":
                        transaction.save(op.model, condition=op.condition)
                    elif op.action == "delete":
                        transaction.delete(op.model, condition=op.condition)
                    else:
                        raise RuntimeError(f"Unsupported DynamoOperation action: {op.action}")
        except Exception as e:
            raise RuntimeError(f"Transaction failed: {e}")

    def execute_query(self, sql: str, _vars: Dict[str, Any] = None) -> Any:
        raise NotImplementedError("execute_query is not supported for DynamoDB")

    def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(response, list):
            return [item.attribute_values for item in response]
        if isinstance(response, Model):
            return response.attribute_values
        return response

    def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None, model_cls: Type[BaseModel] = None) -> Dict[str, Any]:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB get_one")
            
        pynamo_model = self._generate_pynamo_model(table, model_cls)
        try:
            results = self._execute_query_or_scan(pynamo_model, conditions, limit=1)
            # results is an iterator
            for item in results:
                return item.attribute_values
            return None
        except Exception as e:
             raise RuntimeError(f"get_one failed: {e}")

    def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None, limit: int = 100, model_cls: Type[BaseModel] = None) -> List[Dict[str, Any]]:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB get_many")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        try:
            results = self._execute_query_or_scan(pynamo_model, conditions, limit=limit)
            return [item.attribute_values for item in results]
        except Exception as e:
            raise RuntimeError(f"get_many failed: {e}")

    def get_count(self, table: str, conditions: Dict[str, Any], options: Optional[Dict[str, Any]] = None, model_cls: Type[BaseModel] = None) -> int:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB get_count")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        try:
            return self._execute_query_or_scan(pynamo_model, conditions, count_only=True)
        except Exception as e:
            raise RuntimeError(f"get_count failed: {e}")

    def get_move_entity_to_audit_table_query(self, table, entity_id, model_cls: Type[BaseModel] = None):
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB move_entity_to_audit_table")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        audit_table_name = f"{table}_audit"
        pynamo_audit_model = self._generate_pynamo_model(audit_table_name, model_cls, is_audit=True)

        try:
            item = pynamo_model.get(entity_id)
            audit_item = pynamo_audit_model(**item.attribute_values)
            return DynamoOperation("save", audit_item)
        except DoesNotExist:
            return None

    def move_entity_to_audit_table(self, table_name: str, entity_id: str, model_cls: Type[BaseModel] = None):
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB move_entity_to_audit_table")

        pynamo_model = self._generate_pynamo_model(table_name, model_cls)
        audit_table_name = f"{table_name}_audit"
        pynamo_audit_model = self._generate_pynamo_model(audit_table_name, model_cls, is_audit=True)

        try:
            item = pynamo_model.get(entity_id)
            audit_item = pynamo_audit_model(**item.attribute_values)
            audit_item.save()
        except DoesNotExist:
            pass
        except Exception as e:
            raise RuntimeError(f"move_entity_to_audit_table failed: {e}")

    def get_save_query(self, table: str, data: Dict[str, Any], model_cls: Type[BaseModel] = None):
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB save")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        item = pynamo_model(**data)

        # Optimistic locking / create condition:
        # - New records have previous_version == ZERO_UUID (set by prepare_for_save)
        # - Updated records have previous_version == the previous stored version
        previous_version = data.get("previous_version")
        if previous_version and previous_version != "00000000000040008000000000000000":
            condition = (pynamo_model.version == previous_version)
        else:
            condition = pynamo_model.entity_id.does_not_exist()

        return DynamoOperation("save", item, condition=condition)

    def save(self, table: str, data: Dict[str, Any], model_cls: Type[BaseModel] = None) -> Union[Dict[str, Any], None]:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB save")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        item = pynamo_model(**data)
        item.save()
        return item.attribute_values

    def upsert(self, table: str, data: Dict[str, Any], model_cls: Type[BaseModel] = None) -> Union[Dict[str, Any], None]:
        """
        Upsert (update or insert) an item in the specified DynamoDB table.

        This is a simple upsert operation for non-versioned models that replaces
        the entire item based on entity_id (hash key).

        Args:
            table (str): The name of the DynamoDB table.
            data (Dict[str, Any]): The item data (must include 'entity_id').
            model_cls (Type[BaseModel]): The model class for schema mapping.

        Returns:
            Dict[str, Any]: The upserted item's attribute values.

        Raises:
            ValueError: If model_cls is not provided.
            RuntimeError: If entity_id is missing or any DynamoDB operation fails.
        """
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB upsert")

        if 'entity_id' not in data:
            raise RuntimeError("upsert failed: 'entity_id' is required in data")

        try:
            pynamo_model = self._generate_pynamo_model(table, model_cls)
            item = pynamo_model(**data)
            item.save()  # PynamoDB's save() is already an upsert (put_item)
            return item.attribute_values
        except Exception as e:
            raise RuntimeError(f"upsert failed: {e}")

    def delete(self, table: str, data: Dict[str, Any], model_cls: Type[BaseModel] = None) -> bool:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB delete")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        entity_id = data.get('entity_id')
        if entity_id:
            try:
                item = pynamo_model.get(entity_id)
                item.active = False
                item.save()
                return True
            except DoesNotExist:
                return False
        return False

    def hard_delete(self, table: str, entity_id: str, model_cls: Type[BaseModel] = None) -> bool:
        """Permanently deletes a record from the specified table by entity_id."""
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB hard_delete")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        try:
            item = pynamo_model.get(entity_id)
            item.delete()
            return True
        except DoesNotExist:
            return False

    def _execute_query_or_scan(self, model_cls: Type[Model], conditions: Dict[str, Any], limit: int = None, count_only: bool = False):
        """
        Helper to determine whether to use Query or Scan based on conditions.
        """
        # Find hash key and range key using public API instead of _meta
        hash_key_name = None
        range_key_name = None
        
        for name, attr in model_cls.get_attributes().items():
            if getattr(attr, 'is_hash_key', False):
                hash_key_name = name
            if getattr(attr, 'is_range_key', False):
                range_key_name = name

        hash_key_val = conditions.get(hash_key_name) if conditions else None
        
        if hash_key_val is not None:
            # Query path: Hash key is present
            range_key_condition = None
            filter_condition = None
            
            for key, value in conditions.items():
                if key == hash_key_name:
                    continue
                
                attr = getattr(model_cls, key)
                cond = (attr == value)
                
                if key == range_key_name:
                    range_key_condition = cond
                else:
                    if filter_condition is None:
                        filter_condition = cond
                    else:
                        filter_condition = filter_condition & cond
            
            if count_only:
                return model_cls.count(hash_key_val, range_key_condition=range_key_condition, filter_condition=filter_condition)
            else:
                return model_cls.query(hash_key_val, range_key_condition=range_key_condition, filter_condition=filter_condition, limit=limit)
        else:
            # Scan path: Hash key is missing
            scan_condition = None
            if conditions:
                for key, value in conditions.items():
                    attr = getattr(model_cls, key)
                    cond = (attr == value)
                    if scan_condition is None:
                        scan_condition = cond
                    else:
                        scan_condition = scan_condition & cond
            
            if count_only:
                return model_cls.count(filter_condition=scan_condition)
            else:
                return model_cls.scan(scan_condition, limit=limit)
