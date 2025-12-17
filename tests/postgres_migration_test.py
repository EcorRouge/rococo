
import sys
from unittest.mock import MagicMock

# Mock psycopg2 before importing modules that depend on it
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()

import pytest
from rococo.migrations.postgres.migration import PostgresMigration
from rococo.data.postgresql import PostgreSQLAdapter

@pytest.fixture
def mock_db_adapter():
    adapter = MagicMock(spec=PostgreSQLAdapter)
    adapter._database = "test_db"
    # Mocking context manager behavior
    adapter.__enter__.return_value = adapter
    adapter.__exit__.return_value = None
    adapter._connection = MagicMock()
    return adapter

@pytest.fixture
def migration(mock_db_adapter):
    return PostgresMigration(mock_db_adapter)

def test_add_index(migration, mock_db_adapter):
    migration.add_index("test_table", "idx_test", "column1")
    
    expected_query = "CREATE INDEX idx_test ON test_table (column1);"
    mock_db_adapter.execute_query.assert_called_with(expected_query, None)

def test_remove_index(migration, mock_db_adapter):
    # Testing the new signature which does not have table_name
    migration.remove_index("idx_test")
    
    expected_query = "DROP INDEX IF EXISTS idx_test;"
    mock_db_adapter.execute_query.assert_called_with(expected_query, None)

def test_change_column_name(migration, mock_db_adapter):
    migration.change_column_name("test_table", "old_col", "new_col")
    
    expected_query = "ALTER TABLE test_table RENAME COLUMN old_col TO new_col;"
    mock_db_adapter.execute_query.assert_called_with(expected_query, None)
