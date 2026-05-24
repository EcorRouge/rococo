from typing import Any, Dict, List, Optional, Tuple, Type, Union
import os
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, BooleanAttribute, NumberAttribute, JSONAttribute, ListAttribute
from pynamodb.exceptions import DoesNotExist, TransactWriteError
from pynamodb.transactions import TransactWrite
from rococo.data.base import DbAdapter
from rococo.models import BaseModel, VersionedModel
from rococo.models.versioned_model import get_uuid_hex


class DynamoDbOptimisticLockError(RuntimeError):
    """Raised when a DynamoDB versioned save loses its optimistic lock."""


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
                'host': os.getenv('DYNAMODB_ENDPOINT_URL'),
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
        """
        Executes a list of callables. 
        In this adapter, operations_list is expected to be a list of callables (lambdas) 
        returned by get_save_query / get_move_entity_to_audit_table_query.
        """
        for op in operations_list:
            if callable(op):
                op()

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
        return lambda: self.move_entity_to_audit_table(table, entity_id, model_cls)

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
        return lambda: self.save(table, data, model_cls)

    def save(self, table: str, data: Dict[str, Any], model_cls: Type[BaseModel] = None) -> Union[Dict[str, Any], None]:
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB save")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        item = pynamo_model(**data)
        item.save()
        return item.attribute_values

    def save_versioned(
        self,
        table: str,
        data: Dict[str, Any],
        model_cls: Type[VersionedModel] = None,
        write_audit: bool = True
    ) -> Union[Dict[str, Any], None]:
        """
        Atomically save a VersionedModel item and its audit record.

        Existing entities are saved with optimistic locking: the current table
        row must still have the version carried in ``data['previous_version']``.
        If that condition fails, no write is committed. When ``write_audit`` is
        true, the audit row and current row are committed in the same
        transaction. New entities are conditionally inserted so an accidental
        reused ``entity_id`` cannot overwrite an existing item.
        """
        if model_cls is None:
            raise ValueError("model_cls is required for DynamoDB save_versioned")

        entity_id = data.get('entity_id')
        previous_version = data.get('previous_version')
        if not entity_id:
            raise RuntimeError("save_versioned failed: 'entity_id' is required in data")
        if not data.get('version'):
            raise RuntimeError("save_versioned failed: 'version' is required in data")

        pynamo_model = self._generate_pynamo_model(table, model_cls)
        item = pynamo_model(**data)

        try:
            if self._is_initial_version(previous_version):
                with TransactWrite(connection=pynamo_model._get_connection().connection) as transaction:
                    transaction.save(
                        item,
                        condition=pynamo_model.entity_id.does_not_exist()
                    )
                return item.attribute_values

            current_version_condition = pynamo_model.version == previous_version

            audit_item = None
            audit_condition = None
            if write_audit:
                current_values = self._get_current_version_for_audit(
                    pynamo_model,
                    entity_id,
                    previous_version
                )
                audit_table_name = f"{table}_audit"
                pynamo_audit_model = self._generate_pynamo_model(
                    audit_table_name,
                    model_cls,
                    is_audit=True
                )
                audit_item = pynamo_audit_model(**current_values)
                audit_condition = (
                    pynamo_audit_model.entity_id.does_not_exist()
                    & pynamo_audit_model.version.does_not_exist()
                )

            with TransactWrite(connection=pynamo_model._get_connection().connection) as transaction:
                if audit_item is not None:
                    transaction.save(audit_item, condition=audit_condition)

                transaction.save(item, condition=current_version_condition)
            return item.attribute_values
        except DynamoDbOptimisticLockError:
            raise
        except TransactWriteError as e:
            if self._is_optimistic_lock_failure(e):
                raise DynamoDbOptimisticLockError(
                    f"Version conflict while saving entity_id={entity_id} to {table}"
                ) from e
            raise RuntimeError(f"save_versioned failed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"save_versioned failed: {e}") from e

    @staticmethod
    def _is_initial_version(previous_version: Any) -> bool:
        return previous_version in (None, get_uuid_hex(0))

    @staticmethod
    def _is_optimistic_lock_failure(error: TransactWriteError) -> bool:
        return any(
            reason is not None and reason.code == 'ConditionalCheckFailed'
            for reason in error.cancellation_reasons
        )

    @staticmethod
    def _get_current_version_for_audit(
        pynamo_model: Type[Model],
        entity_id: str,
        expected_version: str
    ) -> Dict[str, Any]:
        try:
            current_item = pynamo_model.get(entity_id, consistent_read=True)
        except DoesNotExist as e:
            raise DynamoDbOptimisticLockError(
                f"Cannot update missing DynamoDB entity_id={entity_id}"
            ) from e

        current_values = current_item.attribute_values.copy()
        current_version = current_values.get('version')
        if current_version != expected_version:
            raise DynamoDbOptimisticLockError(
                f"Version conflict for entity_id={entity_id}: expected {expected_version}, found {current_version}"
            )
        return current_values

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
