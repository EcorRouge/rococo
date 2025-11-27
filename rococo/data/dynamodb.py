from typing import Any, Dict, List, Optional, Tuple, Type, Union
from pynamodb.models import Model
from pynamodb.exceptions import DoesNotExist
from rococo.data.base import DbAdapter


class DynamoDbAdapter(DbAdapter):
    """DynamoDB adapter using PynamoDB."""

    def __init__(self, model_registry: Dict[str, Type[Model]]):
        self.model_registry = model_registry

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _get_model(self, table: str) -> Type[Model]:
        if table not in self.model_registry:
            raise ValueError(f"Model for table '{table}' not found in registry.")
        return self.model_registry[table]

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

    def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None) -> Dict[str, Any]:
        model_cls = self._get_model(table)
        try:
            results = self._execute_query_or_scan(model_cls, conditions, limit=1)
            # results is an iterator
            for item in results:
                return item.attribute_values
            return None
        except Exception as e:
             raise RuntimeError(f"get_one failed: {e}")

    def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        model_cls = self._get_model(table)
        try:
            results = self._execute_query_or_scan(model_cls, conditions, limit=limit)
            return [item.attribute_values for item in results]
        except Exception as e:
            raise RuntimeError(f"get_many failed: {e}")

    def get_count(self, table: str, conditions: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> int:
        model_cls = self._get_model(table)
        try:
            return self._execute_query_or_scan(model_cls, conditions, count_only=True)
        except Exception as e:
            raise RuntimeError(f"get_count failed: {e}")

    def get_move_entity_to_audit_table_query(self, table, entity_id):
        return lambda: self.move_entity_to_audit_table(table, entity_id)

    def move_entity_to_audit_table(self, table_name: str, entity_id: str):
        model_cls = self._get_model(table_name)
        audit_table_name = f"{table_name}_audit"
        
        if audit_table_name not in self.model_registry:
             return

        audit_model_cls = self._get_model(audit_table_name)

        try:
            item = model_cls.get(entity_id)
            audit_item = audit_model_cls(**item.attribute_values)
            audit_item.save()
        except DoesNotExist:
            pass
        except Exception as e:
            raise RuntimeError(f"move_entity_to_audit_table failed: {e}")

    def get_save_query(self, table: str, data: Dict[str, Any]):
        return lambda: self.save(table, data)

    def save(self, table: str, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        model_cls = self._get_model(table)
        item = model_cls(**data)
        item.save()
        return item.attribute_values

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        model_cls = self._get_model(table)
        entity_id = data.get('entity_id')
        if entity_id:
            try:
                item = model_cls.get(entity_id)
                item.active = False
                item.save()
                return True
            except DoesNotExist:
                return False
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
