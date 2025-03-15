from ..common.cli_base import BaseCli
from rococo.data.postgresql import PostgreSQLAdapter
from .migration import PostgresMigration

class PostgresCli(BaseCli):
    DB_TYPE = 'postgres'
    REQUIRED_ENV_VARS = ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
    ADAPTER_CLASS = PostgreSQLAdapter
    MIGRATION_CLASS = PostgresMigration

    def get_db_adapter(self, merged_env):
        try:
            return self.ADAPTER_CLASS(
                host=merged_env['POSTGRES_HOST'],
                port=int(merged_env['POSTGRES_PORT']),
                user=merged_env['POSTGRES_USER'],
                password=merged_env['POSTGRES_PASSWORD'],
                database=merged_env['POSTGRES_DB']
            )
        except KeyError as e:
            self.parser.error(f'{e.args[0]} key not found in the environment variables or env files.')
            return


def main():
    cli = PostgresCli()
    cli.run()

if __name__ == "__main__":
    main()
