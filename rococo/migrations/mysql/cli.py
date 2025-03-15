from ..common.cli_base import BaseCli
from rococo.data.mysql import MySqlAdapter
from .migration import MySQLMigration


class MysqlCli(BaseCli):
    DB_TYPE = 'mysql'
    REQUIRED_ENV_VARS = ['MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    ADAPTER_CLASS = MySqlAdapter
    MIGRATION_CLASS = MySQLMigration

    def get_db_adapter(self, merged_env):
        try:
            return self.ADAPTER_CLASS(
                    host=merged_env['MYSQL_HOST'],
                    port=int(merged_env['MYSQL_PORT']),
                    user=merged_env['MYSQL_USER'],
                    password=merged_env['MYSQL_PASSWORD'],
                    database=merged_env['MYSQL_DATABASE']
                )
        except KeyError as e:
            self.parser.error(f"{e.args[0]} key not found in environment variables.")
            return None


def main():
    cli = MysqlCli()
    cli.run()

if __name__ == "__main__":
    main()
