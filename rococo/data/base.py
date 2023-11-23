from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Union


class DbAdapter(ABC):
    """Abstract base class for database adapters."""

    @abstractmethod
    def __enter__(self) -> 'DbAdapter':
        """Context manager entry point for preparing DB connection."""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point for closing DB connection."""
        pass

    @abstractmethod
    def execute_query(self, sql: str, _vars: Dict[str, Any] = None) -> Any:
        """Executes a raw SQL query against the DB."""
        pass

    @abstractmethod
    def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Parses the raw response from the database and returns structured data."""
        pass

    @abstractmethod
    def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None) -> Dict[str, Any]:
        """Fetches a single record from the specified table based on given conditions."""
        pass

    @abstractmethod
    def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None, 
                 limit: int = 100) -> List[Dict[str, Any]]:
        """Fetches multiple records from the specified table based on given conditions."""
        pass

    @abstractmethod
    def move_entity_to_audit_table(self, table_name: str, entity_id: str):
        """Inserts the existing entities by entity_id in {table_name}_audit table."""
        pass

    @abstractmethod
    def save(self, table: str, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        """Saves or updates a record in the specified table."""
        pass

    @abstractmethod
    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        """Deletes a record in the specified table based on given conditions."""
        pass
