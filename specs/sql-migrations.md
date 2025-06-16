# Rococo MySQL Migrations Spec

This spec defines how to write and manage schema migrations using Rococo's internal `BaseMigration` / `MySQLMigration` tooling. Migrations are Python files that mutate the schema and optionally seed/update data.


## ✅ Structure of a Migration File

Each migration file must define:

```python
revision = "0000000001"
down_revision = "0000000000"


def upgrade(migration):
    # Schema changes go here
    ...
    migration.update_version_table(version=revision)

def downgrade(migration):
    # Reverse the schema changes
    ...
    migration.update_version_table(version=down_revision)

```

Where migration is the `MySQLMigration` object passed to the `upgrade` and `downgrade` functions.

## MySQLMigration Object
The `MySQLMigration` object provides methods to perform schema changes. It has the following methods:

```python
    def _does_column_exist(self, table_name: str, column_name: str, commit: bool = True) -> bool:
        ...

    def _does_primary_key_constraint_exists(self, table_name: str, commit: bool = True) -> bool:
        ...

    def remove_primary_key(self, table_name: str, commit: bool = True):
        ...

    def add_primary_key(self, table_name: str, keys: str, commit: bool = True):
        ...

    def add_column(self, table_name: str, column_name: str, datatype: str, commit: bool = True):
        ...

    def drop_column(self, table_name: str, column_name: str, commit: bool = True):
        ...

    def alter_index(self, table_name: str, new_index_name: str, new_indexed_column: str, old_index_name: str, commit: bool = True):
        ...

    def add_index(self, table_name: str, index_name: str, indexed_column: str, commit: bool = True):
        ...

    def remove_index(self, table_name: str, index_name: str, commit: bool = True):
        ...

    def alter_column(self, table_name: str, column_name: str, datatype: str, commit: bool = True):
        ...

    def change_column_name(self, table_name: str, old_column_name: str, new_column_name: str, commit: bool = True):
        ...

    def change_table_name(self, old_table: str, new_table: str, commit: bool = True):
        ...

    def create_table(self, table_name: str, fields_with_type: str, drop_if_exists: bool = True, commit: bool = True):
        ...

    def drop_table(self, table_name: str, commit: bool = True):
        ...

    def insert_db_version_data(self, commit: bool = True):
        ...

    def update_version_table(self, version: str, commit: bool = True):
        ...
```

## ✅ Migration File Naming
Each migration file must be named as `<revision>_<down_revision>.py` and placed in the `flask/app/migrations` directory.

The revision and down_revision are the revision numbers defined in the migration file. The revision number must be unique and follow a sequential order. You will be given a revision number in the format of `1` or `25` in the prompt which should translate to `0000000001` or `0000000025` respectively. The down_revision is revision number minus 1.


## ✅ Examples


### Example 1: Table creation

```python

revision = "0000000001"
down_revision = "0000000000"



def upgrade(migration):
    # write migration here
    migration.create_table(
        "organization",
        """
            "entity_id" varchar(32) NOT NULL,
            "version" varchar(32) NOT NULL,
            "previous_version" varchar(32) DEFAULT '00000000000000000000000000000000',
            "active" boolean DEFAULT true,
            "changed_by_id" varchar(32) DEFAULT NULL,
            "changed_on" timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            "name" varchar(128) NOT NULL,
            "code" varchar(16) DEFAULT NULL,
            "description" TEXT DEFAULT NULL,
            PRIMARY KEY ("entity_id")
        """
    )
    migration.add_index("organization", "organization_name_ind", "name")

    # Create the "organization_audit" table
    migration.create_table(
        "organization_audit",
        """
            "entity_id" varchar(32) NOT NULL,
            "version" varchar(32) NOT NULL,
            "previous_version" varchar(32) DEFAULT '00000000000000000000000000000000',
            "active" boolean DEFAULT true,
            "changed_by_id" varchar(32) DEFAULT NULL,
            "changed_on" timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            "name" varchar(128) NOT NULL,
            "code" varchar(16) DEFAULT NULL,
            "description" TEXT DEFAULT NULL,
            PRIMARY KEY ("entity_id", "version")
        """
    )
    migration.update_version_table(version=revision)


def downgrade(migration):
    # write migration here
    migration.drop_table(table_name="organization")
    migration.drop_table(table_name="organization_audit")

    migration.update_version_table(version=down_revision)
```

- Each new table must have the following columns:
  - `"entity_id" varchar(32) NOT NULL`
  - `"version" varchar(32) NOT NULL`
  - `"previous_version" varchar(32) DEFAULT '00000000000000000000000000000000'`
  - `"active" boolean DEFAULT true`
  - `"changed_by_id" varchar(32) DEFAULT NULL`
  - `"changed_on" timestamp NULL DEFAULT CURRENT_TIMESTAMP`

- All tables must have a corresponding audit table with the same columns as the main table. The main table should have a primary key only on `entity_id`. The audit table should have a composite primary key on `("entity_id", "version")`. All schema changes must be applied to both the main and audit tables. The audit table should be named `<table_name>_audit`.


### Example 2: Table alteration

```python
revision = "0000000027"
down_revision = "0000000026"


def upgrade(migration):
    # write migration here
    migration.add_column(table_name="customer_application", column_name="broker_id", datatype="VARCHAR(32) DEFAULT NULL")
    migration.add_column(table_name="customer_application_audit", column_name="broker_id", datatype="VARCHAR(32) DEFAULT NULL")

    migration.execute("""
        UPDATE customer_application ca
        JOIN email e ON ca.broker_email = e.email
        JOIN person p ON e.person = p.entity_id
        SET ca.broker_id = p.entity_id;
    """)

    migration.update_version_table(version=revision)


def downgrade(migration):
    # write migration here
    migration.drop_column("customer_application", "broker_id")
    migration.drop_column("customer_application_audit", "broker_id")

    migration.update_version_table(version=down_revision)
```

- The `upgrade` function should contain the schema changes to be applied.
- The `downgrade` function should contain the schema changes to be reverted.
- The `migration.execute` method can be used to execute raw SQL queries.
- Any schema changes such as adding or dropping columns should be applied to both the main and audit tables.
- The `migration.update_version_table` method should be called at the end of the `upgrade` and `downgrade` functions to update the version table with the new revision number.