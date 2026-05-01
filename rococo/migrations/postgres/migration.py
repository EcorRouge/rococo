from ..common.sql_migration_base import SQLMigrationBase
from rococo.data.postgresql import PostgreSQLAdapter


class PostgresMigration(SQLMigrationBase):
    EXISTS_COLUMN_QUERY = (
        "SELECT COUNT(*) AS count "
        "FROM information_schema.columns "
        "WHERE table_catalog = %s AND table_name = %s AND column_name = %s;"
    )
    EXISTS_PK_QUERY = (
        "SELECT COUNT(*) AS count "
        "FROM information_schema.table_constraints "
        "WHERE table_catalog = %s AND table_name = %s "
        "AND constraint_type = 'PRIMARY KEY';"
    )
    UPDATE_VERSION_QUERY = "UPDATE db_version SET version = %s;"
    INSERT_DB_VERSION_DATA_QUERY = (
        "INSERT INTO db_version (version) VALUES ('0000000000');"
    )

    DROP_PK_TEMPLATE = (
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_pkey;"
    )
    ADD_PK_TEMPLATE = (
        "ALTER TABLE {table} ADD CONSTRAINT {table}_pkey PRIMARY KEY {keys};"
    )
    ADD_COLUMN_TEMPLATE = "ALTER TABLE {table} ADD COLUMN {column} {datatype};"
    DROP_COLUMN_TEMPLATE = "ALTER TABLE {table} DROP COLUMN {column};"
    ADD_INDEX_TEMPLATE = "CREATE INDEX {index} ON {table} ({column});"
    REMOVE_INDEX_TEMPLATE = "DROP INDEX IF EXISTS {index};"
    ALTER_INDEX_TEMPLATES = (
        "DROP INDEX IF EXISTS {old_index};",
        "CREATE INDEX {new_index} ON {table} ({new_column});",
    )
    ALTER_COLUMN_TEMPLATE = "ALTER TABLE {table} ALTER COLUMN {column} TYPE {datatype};"
    RENAME_COLUMN_TEMPLATE = "ALTER TABLE {table} RENAME COLUMN {old} TO {new};"
    RENAME_TABLE_TEMPLATE = "ALTER TABLE {old_table} RENAME TO {new_table};"
    CREATE_TABLE_TEMPLATE = "CREATE TABLE {table} ({fields_with_type});"
    DROP_TABLE_IF_EXISTS_TEMPLATE = "DROP TABLE IF EXISTS {table} CASCADE;"
    DROP_TABLE_TEMPLATE = "DROP TABLE {table} CASCADE;"

    def __init__(self, db_adapter: PostgreSQLAdapter):
        super().__init__(db_adapter)
