import logging
import re
from typing import Any, Iterable, Optional

from .migration_base import MigrationBase


class SQLMigrationBase(MigrationBase):
    """Shared SQL migration helpers for relational adapters.

    Concrete subclasses (e.g. PostgresMigration, MySQLMigration) provide the
    dialect-specific SQL via class-level templates and queries. Identifiers
    (table, column, index, constraint names) are validated against
    ``_IDENT_RE`` before being interpolated into SQL. Values (database name,
    version strings) are bound through the adapter's parameterized query
    interface, never concatenated into SQL strings.
    """

    # Parameterized queries (use %s placeholders).
    EXISTS_COLUMN_QUERY: str = NotImplemented      # args: (database, table, column)
    EXISTS_PK_QUERY: str = NotImplemented          # args: (database, table)
    UPDATE_VERSION_QUERY: str = NotImplemented     # args: (version,)
    INSERT_DB_VERSION_DATA_QUERY: str = NotImplemented  # no args

    # Identifier templates (use str.format with validated names).
    DROP_PK_TEMPLATE: str = NotImplemented         # {table}
    ADD_PK_TEMPLATE: str = NotImplemented          # {table}, {keys}
    ADD_COLUMN_TEMPLATE: str = NotImplemented      # {table}, {column}, {datatype}
    DROP_COLUMN_TEMPLATE: str = NotImplemented     # {table}, {column}
    ADD_INDEX_TEMPLATE: str = NotImplemented       # {table}, {index}, {column}
    REMOVE_INDEX_TEMPLATE: str = NotImplemented    # {table}, {index}
    ALTER_INDEX_TEMPLATES: tuple = NotImplemented  # tuple of templates run in order
    ALTER_COLUMN_TEMPLATE: str = NotImplemented    # {table}, {column}, {datatype}
    RENAME_COLUMN_TEMPLATE: str = NotImplemented   # {table}, {old}, {new}
    RENAME_TABLE_TEMPLATE: str = NotImplemented    # {old_table}, {new_table}
    CREATE_TABLE_TEMPLATE: str = NotImplemented    # {table}, {fields_with_type}
    DROP_TABLE_IF_EXISTS_TEMPLATE: str = NotImplemented  # {table}
    DROP_TABLE_TEMPLATE: str = NotImplemented      # {table}

    _IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def _validate_ident(self, name: str) -> str:
        if not isinstance(name, str) or not self._IDENT_RE.match(name):
            raise ValueError(f"invalid SQL identifier: {name!r}")
        return name

    def _validate_idents(self, names: Iterable[str]) -> None:
        for n in names:
            self._validate_ident(n)

    def _validate_pk_keys(self, keys: str) -> str:
        if not isinstance(keys, str) or not (keys.startswith("(") and keys.endswith(")")):
            raise ValueError(
                f"primary key keys must be wrapped in parentheses, got: {keys!r}"
            )
        inner = keys[1:-1]
        for col in inner.split(","):
            self._validate_ident(col.strip())
        return keys

    @staticmethod
    def _extract_count(response: Any) -> int:
        if not response:
            return 0
        row = response[0] if isinstance(response, list) else response
        if not isinstance(row, dict) or not row:
            return 0
        value = next(iter(row.values()))
        return int(value or 0)

    def _does_column_exist(self, table_name, column_name, commit: bool = True) -> bool:
        self._validate_idents([table_name, column_name])
        rows = self.execute(
            self.EXISTS_COLUMN_QUERY,
            commit=commit,
            args=(self.db_adapter._database, table_name, column_name),
        )
        return self._extract_count(rows) > 0

    def _does_primary_key_constraint_exists(self, table_name, commit: bool = True) -> bool:
        self._validate_ident(table_name)
        rows = self.execute(
            self.EXISTS_PK_QUERY,
            commit=commit,
            args=(self.db_adapter._database, table_name),
        )
        return self._extract_count(rows) > 0

    def remove_primary_key(self, table_name, commit: bool = True):
        self._validate_ident(table_name)
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            return self.execute(
                self.DROP_PK_TEMPLATE.format(table=table_name), commit=commit
            )
        logging.info(
            f"PRIMARY KEY constraint for table {table_name} does not exist. "
            f"Skipping removal..."
        )

    def add_primary_key(self, table_name, keys: str, commit: bool = True):
        self._validate_ident(table_name)
        self._validate_pk_keys(keys)
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            self.execute(
                self.DROP_PK_TEMPLATE.format(table=table_name), commit=commit
            )
        return self.execute(
            self.ADD_PK_TEMPLATE.format(table=table_name, keys=keys), commit=commit
        )

    def add_column(self, table_name, column_name, datatype, commit: bool = True):
        self._validate_idents([table_name, column_name])
        if self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(
                f"Column {column_name} for table {table_name} already exists. "
                f"Skipping creation..."
            )
            return
        query = self.ADD_COLUMN_TEMPLATE.format(
            table=table_name, column=column_name, datatype=datatype
        )
        self.execute(query, commit=commit)

    def drop_column(self, table_name, column_name, commit: bool = True):
        self._validate_idents([table_name, column_name])
        if not self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(
                f"Column {column_name} for table {table_name} does not exist. "
                f"Skipping deletion..."
            )
            return
        query = self.DROP_COLUMN_TEMPLATE.format(
            table=table_name, column=column_name
        )
        self.execute(query, commit=commit)

    def alter_index(
        self,
        table_name,
        new_index_name,
        new_indexed_column,
        old_index_name,
        commit: bool = True,
    ):
        self._validate_idents(
            [table_name, new_index_name, new_indexed_column, old_index_name]
        )
        for tmpl in self.ALTER_INDEX_TEMPLATES:
            query = tmpl.format(
                table=table_name,
                new_index=new_index_name,
                new_column=new_indexed_column,
                old_index=old_index_name,
            )
            self.execute(query, commit=commit)

    def add_index(self, table_name, index_name, indexed_column, commit: bool = True):
        self._validate_idents([table_name, index_name, indexed_column])
        query = self.ADD_INDEX_TEMPLATE.format(
            table=table_name, index=index_name, column=indexed_column
        )
        self.execute(query, commit=commit)

    def remove_index(self, table_name, index_name, commit: bool = True):
        self._validate_idents([table_name, index_name])
        query = self.REMOVE_INDEX_TEMPLATE.format(
            table=table_name, index=index_name
        )
        self.execute(query, commit=commit)

    def alter_column(self, table_name, column_name, datatype, commit: bool = True):
        self._validate_idents([table_name, column_name])
        query = self.ALTER_COLUMN_TEMPLATE.format(
            table=table_name, column=column_name, datatype=datatype
        )
        self.execute(query, commit=commit)

    def change_column_name(
        self, table_name, old_column_name, new_column_name, commit: bool = True
    ):
        self._validate_idents([table_name, old_column_name, new_column_name])
        query = self.RENAME_COLUMN_TEMPLATE.format(
            table=table_name, old=old_column_name, new=new_column_name
        )
        self.execute(query, commit=commit)

    def change_table_name(self, old_table, new_table, commit: bool = True):
        self._validate_idents([old_table, new_table])
        query = self.RENAME_TABLE_TEMPLATE.format(
            old_table=old_table, new_table=new_table
        )
        self.execute(query, commit=commit)

    def create_table(
        self, table_name, fields_with_type, drop_if_exists: bool = True, commit: bool = True
    ):
        self._validate_ident(table_name)
        if drop_if_exists:
            drop_query = self.DROP_TABLE_IF_EXISTS_TEMPLATE.format(table=table_name)
            self.execute(drop_query, commit=commit)
        create_query = self.CREATE_TABLE_TEMPLATE.format(
            table=table_name, fields_with_type=fields_with_type
        )
        self.execute(create_query, commit=commit)

    def drop_table(self, table_name, commit: bool = True):
        self._validate_ident(table_name)
        query = self.DROP_TABLE_TEMPLATE.format(table=table_name)
        self.execute(query, commit=commit)

    def insert_db_version_data(self, commit: bool = True):
        self.execute(self.INSERT_DB_VERSION_DATA_QUERY, commit=commit)

    def update_version_table(self, version, commit: bool = True):
        self.execute(self.UPDATE_VERSION_QUERY, commit=commit, args=(str(version),))
