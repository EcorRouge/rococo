"""
Tests for database adapter abstract base class.

This module tests the abstract DbAdapter base class that defines
the interface contract for database adapters.
"""
import unittest
from abc import ABC
from typing import Dict, Any, List, Tuple, Union, Optional

from rococo.data.base import DbAdapter


class TestDbAdapterAbstractClass(unittest.TestCase):
    """Test DbAdapter abstract base class."""

    def test_db_adapter_is_abstract(self):
        """
        Test that DbAdapter is an abstract base class.

        Verifies it inherits from ABC.
        """
        self.assertTrue(issubclass(DbAdapter, ABC))

    def test_db_adapter_cannot_be_instantiated_directly(self):
        """
        Test that DbAdapter cannot be instantiated without implementing abstract methods.

        Verifies abstract class enforcement.
        """
        with self.assertRaises(TypeError) as context:
            DbAdapter()

        self.assertIn("abstract", str(context.exception).lower())

    def test_db_adapter_has_all_abstract_methods_defined(self):
        """
        Test that DbAdapter has all required abstract methods defined.

        Verifies the complete interface contract.
        """
        abstract_methods = DbAdapter.__abstractmethods__

        expected_methods = {
            '__enter__',
            '__exit__',
            'run_transaction',
            'execute_query',
            'parse_db_response',
            'get_one',
            'get_many',
            'get_count',
            'get_move_entity_to_audit_table_query',
            'move_entity_to_audit_table',
            'get_save_query',
            'save',
            'delete'
        }

        for method in expected_methods:
            self.assertIn(method, abstract_methods)

    def test_db_adapter_can_be_subclassed_with_all_methods(self):
        """
        Test that DbAdapter can be subclassed when all abstract methods are implemented.

        Verifies complete implementation works.
        """
        class CompleteDbAdapter(DbAdapter):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def run_transaction(self, operations_list: List[Any]):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def execute_query(self, sql: str, _vars: Dict[str, Any] = None) -> Any:
                return {}

            def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
                return {}

            def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None) -> Dict[str, Any]:
                return {}

            def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
                return []

            def get_count(self, table: str, conditions: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> int:
                return 0

            def get_move_entity_to_audit_table_query(self, table, entity_id):
                return "QUERY"

            def move_entity_to_audit_table(self, table_name: str, entity_id: str):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def get_save_query(self, table: str, data: Dict[str, Any]):
                return "QUERY"

            def save(self, table: str, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
                return data

            def delete(self, table: str, data: Dict[str, Any]) -> bool:
                return True

        # Should not raise
        adapter = CompleteDbAdapter()
        self.assertIsInstance(adapter, DbAdapter)

    def test_incomplete_db_adapter_cannot_be_instantiated(self):
        """
        Test that incomplete DbAdapter subclasses cannot be instantiated.

        Verifies abstract method enforcement.
        """
        class IncompleteDbAdapter(DbAdapter):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                pass

            # Missing most other abstract methods

        with self.assertRaises(TypeError) as context:
            IncompleteDbAdapter()

        self.assertIn("abstract", str(context.exception).lower())

    def test_db_adapter_works_as_context_manager(self):
        """
        Test that DbAdapter subclass works as context manager.

        Verifies context manager protocol functionality.
        """
        class TestDbAdapter(DbAdapter):
            def __init__(self):
                self.entered = False
                self.exited = False

            def __enter__(self):
                self.entered = True
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.exited = True

            def run_transaction(self, operations_list: List[Any]):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def execute_query(self, sql: str, _vars: Dict[str, Any] = None) -> Any:
                return {}

            def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
                return {}

            def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None) -> Dict[str, Any]:
                return {}

            def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
                return []

            def get_count(self, table: str, conditions: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> int:
                return 0

            def get_move_entity_to_audit_table_query(self, table, entity_id):
                return "QUERY"

            def move_entity_to_audit_table(self, table_name: str, entity_id: str):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def get_save_query(self, table: str, data: Dict[str, Any]):
                return "QUERY"

            def save(self, table: str, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
                return data

            def delete(self, table: str, data: Dict[str, Any]) -> bool:
                return True

        with TestDbAdapter() as adapter:
            self.assertTrue(adapter.entered)

        self.assertTrue(adapter.exited)


if __name__ == '__main__':
    unittest.main()
