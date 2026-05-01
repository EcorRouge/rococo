from ..common.sql_migration_base import SQLMigrationBase
from rococo.data.mysql import MySqlAdapter


class MySQLMigration(SQLMigrationBase):
    EXISTS_COLUMN_QUERY = (
        "SELECT COUNT(*) AS count "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s;"
    )
    EXISTS_PK_QUERY = (
        "SELECT COUNT(*) AS count "
        "FROM information_schema.TABLE_CONSTRAINTS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "AND CONSTRAINT_TYPE = 'PRIMARY KEY';"
    )
    UPDATE_VERSION_QUERY = "UPDATE db_version SET version = %s;"
    INSERT_DB_VERSION_DATA_QUERY = (
        "INSERT INTO db_version (version) VALUES ('0000000000');"
    )

    DROP_PK_TEMPLATE = "ALTER TABLE {table} DROP PRIMARY KEY;"
    ADD_PK_TEMPLATE = "ALTER TABLE {table} ADD PRIMARY KEY {keys};"
    ADD_COLUMN_TEMPLATE = "ALTER TABLE {table} ADD {column} {datatype};"
    DROP_COLUMN_TEMPLATE = "ALTER TABLE {table} DROP {column};"
    ADD_INDEX_TEMPLATE = "ALTER TABLE {table} ADD INDEX {index} ({column});"
    REMOVE_INDEX_TEMPLATE = "ALTER TABLE {table} DROP INDEX {index};"
    ALTER_INDEX_TEMPLATES = (
        "ALTER TABLE {table} ADD INDEX {new_index} ({new_column}), "
        "DROP INDEX {old_index};",
    )
    ALTER_COLUMN_TEMPLATE = "ALTER TABLE {table} MODIFY COLUMN {column} {datatype};"
    RENAME_COLUMN_TEMPLATE = "ALTER TABLE {table} RENAME COLUMN {old} TO {new};"
    RENAME_TABLE_TEMPLATE = "ALTER TABLE {old_table} RENAME TO {new_table};"
    CREATE_TABLE_TEMPLATE = "CREATE TABLE {table} ({fields_with_type});"
    DROP_TABLE_IF_EXISTS_TEMPLATE = "DROP TABLE IF EXISTS {table};"
    DROP_TABLE_TEMPLATE = "DROP TABLE {table};"

    def __init__(self, db_adapter: MySqlAdapter):
        super().__init__(db_adapter)
