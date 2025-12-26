import sys
from unittest.mock import MagicMock, patch

import pytest


# Create mock objects for psycopg2
_mock_psycopg2 = MagicMock()
_mock_psycopg2_extras = MagicMock()


@pytest.fixture
def mock_db_adapter():
    """Fixture that provides a mocked PostgreSQLAdapter."""
    with patch.dict('sys.modules', {
        'psycopg2': _mock_psycopg2,
        'psycopg2.extras': _mock_psycopg2_extras
    }):
        from rococo.data.postgresql import PostgreSQLAdapter
        adapter = MagicMock(spec=PostgreSQLAdapter)
        adapter._database = "test_db"
        # Mocking context manager behavior
        adapter.__enter__.return_value = adapter
        adapter.__exit__.return_value = None
        adapter._connection = MagicMock()
        yield adapter


@pytest.fixture
def migration(mock_db_adapter):
    """Fixture that provides PostgresMigration with psycopg2 mocked."""
    with patch.dict('sys.modules', {
        'psycopg2': _mock_psycopg2,
        'psycopg2.extras': _mock_psycopg2_extras
    }):
        from rococo.migrations.postgres.migration import PostgresMigration
        yield PostgresMigration(mock_db_adapter)

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
