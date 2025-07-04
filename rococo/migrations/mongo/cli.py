# rococo/migrations/mongo/cli.py
from rococo.migrations.common.cli_base import BaseCli
from rococo.data.mongodb import MongoDBAdapter
from .migration import MongoMigration  # This will be a new class


class MongoCli(BaseCli):
    DB_TYPE = 'mongodb'
    # Define environment variables required for MongoDB connection
    REQUIRED_ENV_VARS = ['MONGO_URI', 'MONGO_DATABASE']
    ADAPTER_CLASS = MongoDBAdapter
    MIGRATION_CLASS = MongoMigration

    def get_db_adapter(self, merged_env):
        try:
            return self.ADAPTER_CLASS(
                mongo_uri=merged_env['MONGO_URI'],
                mongo_database=merged_env['MONGO_DATABASE']
                # Add any other client options from env if needed
            )
        except KeyError as e:
            self.parser.error(
                f'{e.args[0]} key not found in the environment variables or env files.')
            return None


def main():
    cli = MongoCli()
    cli.run()


if __name__ == "__main__":
    main()
