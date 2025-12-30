"""
Test cases for BaseCli functionality.
"""

import os
import sys
import pytest
import argparse
from collections import OrderedDict
from unittest.mock import MagicMock, Mock, patch, call
from rococo.migrations.common.cli_base import BaseCli


class ParserErrorException(Exception):
    """Custom exception for testing parser errors instead of SystemExit."""
    pass


class ConcreteCli(BaseCli):
    """Concrete implementation of BaseCli for testing purposes."""
    DB_TYPE = 'test_db'
    REQUIRED_ENV_VARS = ['TEST_HOST', 'TEST_PORT', 'TEST_USER', 'TEST_PASSWORD']
    ADAPTER_CLASS = MagicMock
    MIGRATION_CLASS = MagicMock

    def get_db_adapter(self, merged_env):
        """Test implementation of abstract method."""
        return self.ADAPTER_CLASS(
            host=merged_env.get('TEST_HOST'),
            port=merged_env.get('TEST_PORT'),
            user=merged_env.get('TEST_USER'),
            password=merged_env.get('TEST_PASSWORD')
        )


def create_mock_args(command=None, migrations_dir=None, env_files=None):
    """Create mock argparse.Namespace for testing."""
    args = argparse.Namespace()
    args.command = command
    args.migrations_dir = migrations_dir
    args.env_files = env_files if env_files is not None else []
    return args


class TestCreateParser:
    """Test cases for _create_parser method."""

    def test_parser_has_migrations_dir_argument(self):
        """Test that parser has --migrations-dir argument."""
        cli = ConcreteCli()

        # Parse with --migrations-dir
        args = cli.parser.parse_args(['--migrations-dir', '/some/path', 'version'])

        assert args.migrations_dir == '/some/path'

    def test_parser_has_env_files_argument(self):
        """Test that parser has --env-files argument and accepts multiple values."""
        cli = ConcreteCli()

        # Test with multiple env files (use migrations-dir to terminate the env-files list)
        args = cli.parser.parse_args(['--env-files', 'file1.env', 'file2.env', '--migrations-dir', '/tmp', 'version'])

        assert args.env_files == ['file1.env', 'file2.env']
        assert args.migrations_dir == '/tmp'
        assert args.command == 'version'

    def test_parser_has_all_subcommands(self):
        """Test that parser has all required subcommands."""
        cli = ConcreteCli()

        # Test 'new' subcommand
        args = cli.parser.parse_args(['new'])
        assert args.command == 'new'

        # Test 'rf' subcommand
        args = cli.parser.parse_args(['rf'])
        assert args.command == 'rf'

        # Test 'rb' subcommand
        args = cli.parser.parse_args(['rb'])
        assert args.command == 'rb'

        # Test 'version' subcommand
        args = cli.parser.parse_args(['version'])
        assert args.command == 'version'

    def test_parser_description_includes_db_type(self):
        """Test that parser description includes DB_TYPE."""
        cli = ConcreteCli()

        assert 'test_db' in cli.parser.description

    def test_parser_defaults(self):
        """Test that parser sets correct default values."""
        cli = ConcreteCli()
        args = cli.parser.parse_args(['version'])

        assert args.migrations_dir is None
        assert args.env_files == []


class TestLoadFromCliArgs:
    """Test cases for _load_from_cli_args method."""

    def test_load_from_cli_args_single_file(self):
        """Test loading env vars from a single file."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['test.env'])
        env_data = {'KEY1': 'value1', 'KEY2': 'value2'}

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=True), \
             patch('rococo.migrations.common.cli_base.dotenv_values', return_value=env_data):

            result = cli._load_from_cli_args(args)

            assert isinstance(result, OrderedDict)
            assert result['KEY1'] == 'value1'
            assert result['KEY2'] == 'value2'

    def test_load_from_cli_args_multiple_files(self):
        """Test loading env vars from multiple files with correct merge order."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['file1.env', 'file2.env'])

        def mock_dotenv(file_path):
            if 'file1.env' in file_path:
                return {'KEY1': 'from_file1', 'KEY2': 'from_file1'}
            elif 'file2.env' in file_path:
                return {'KEY2': 'from_file2', 'KEY3': 'from_file2'}
            return {}

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=True), \
             patch('rococo.migrations.common.cli_base.dotenv_values', side_effect=mock_dotenv):

            result = cli._load_from_cli_args(args)

            assert result['KEY1'] == 'from_file1'
            assert result['KEY2'] == 'from_file2'  # Later file overrides
            assert result['KEY3'] == 'from_file2'

    def test_load_from_cli_args_file_not_found(self):
        """Test error when env file does not exist."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['missing.env'])

        # Mock parser.error to raise custom exception
        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=False):
            with pytest.raises(ParserErrorException, match="missing.env file not found"):
                cli._load_from_cli_args(args)

    def test_load_from_cli_args_file_is_directory(self):
        """Test error when env file path is a directory."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['not_a_file'])

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=False):
            with pytest.raises(ParserErrorException, match="not_a_file file not found"):
                cli._load_from_cli_args(args)

    def test_load_from_cli_args_empty_list(self):
        """Test loading from empty env_files list."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=[])

        result = cli._load_from_cli_args(args)

        assert isinstance(result, OrderedDict)
        assert len(result) == 0

    def test_load_from_cli_args_later_files_override_earlier(self):
        """Test that values from later files take precedence."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['a.env', 'b.env', 'c.env'])

        def mock_dotenv(file_path):
            if 'a.env' in file_path:
                return {'SHARED': 'from_a', 'A_ONLY': 'a_value'}
            elif 'b.env' in file_path:
                return {'SHARED': 'from_b', 'B_ONLY': 'b_value'}
            elif 'c.env' in file_path:
                return {'SHARED': 'from_c', 'C_ONLY': 'c_value'}
            return {}

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=True), \
             patch('rococo.migrations.common.cli_base.dotenv_values', side_effect=mock_dotenv):

            result = cli._load_from_cli_args(args)

            assert result['SHARED'] == 'from_c'  # Last file wins
            assert result['A_ONLY'] == 'a_value'
            assert result['B_ONLY'] == 'b_value'
            assert result['C_ONLY'] == 'c_value'


class TestLoadFromSecrets:
    """Test cases for _load_from_secrets method."""

    def test_load_from_secrets_valid_flow(self):
        """Test successful loading from .env.secrets and APP_ENV file."""
        cli = ConcreteCli()
        secrets_data = {'APP_ENV': 'production', 'SECRET_KEY': 'secret_value'}
        app_env_data = {'DB_HOST': 'localhost', 'SECRET_KEY': 'overridden'}

        def mock_exists(path):
            return path in ['.env.secrets', 'production.env']

        def mock_isfile(path):
            return path in ['.env.secrets', 'production.env']

        def mock_dotenv(path):
            if path == '.env.secrets':
                return secrets_data
            elif path == 'production.env':
                return app_env_data
            return {}

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isfile', side_effect=mock_isfile), \
             patch('rococo.migrations.common.cli_base.dotenv_values', side_effect=mock_dotenv):

            result = cli._load_from_secrets()

            assert isinstance(result, OrderedDict)
            assert result['APP_ENV'] == 'production'
            assert result['SECRET_KEY'] == 'overridden'  # APP_ENV file overrides secrets
            assert result['DB_HOST'] == 'localhost'

    def test_load_from_secrets_file_missing(self):
        """Test error when .env.secrets file does not exist."""
        cli = ConcreteCli()

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=False):
            with pytest.raises(ParserErrorException, match=".env.secrets not found"):
                cli._load_from_secrets()

    def test_load_from_secrets_file_is_directory(self):
        """Test error when .env.secrets is a directory."""
        cli = ConcreteCli()

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=False):
            with pytest.raises(ParserErrorException, match=".env.secrets not found"):
                cli._load_from_secrets()

    def test_load_from_secrets_missing_app_env(self):
        """Test error when APP_ENV is not in secrets file."""
        cli = ConcreteCli()
        secrets_data = {'SOME_KEY': 'some_value'}  # Missing APP_ENV

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isfile', return_value=True), \
             patch('rococo.migrations.common.cli_base.dotenv_values', return_value=secrets_data):
            with pytest.raises(ParserErrorException, match="APP_ENV not found"):
                cli._load_from_secrets()

    def test_load_from_secrets_app_env_file_missing(self):
        """Test error when APP_ENV file does not exist."""
        cli = ConcreteCli()
        secrets_data = {'APP_ENV': 'missing_env'}

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        def mock_exists(path):
            if path == '.env.secrets':
                return True
            return False

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isfile', return_value=True), \
             patch('rococo.migrations.common.cli_base.dotenv_values', return_value=secrets_data):
            with pytest.raises(ParserErrorException, match="APP_ENV not found.*or corresponding .env file missing"):
                cli._load_from_secrets()

    def test_load_from_secrets_app_env_is_directory(self):
        """Test error when APP_ENV file is a directory."""
        cli = ConcreteCli()
        secrets_data = {'APP_ENV': 'dir_env'}

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        def mock_exists(path):
            return path in ['.env.secrets', 'dir_env.env']

        def mock_isfile(path):
            if path == '.env.secrets':
                return True
            return False  # dir_env.env is not a file

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isfile', side_effect=mock_isfile), \
             patch('rococo.migrations.common.cli_base.dotenv_values', return_value=secrets_data):
            with pytest.raises(ParserErrorException, match="APP_ENV not found.*or corresponding .env file missing"):
                cli._load_from_secrets()

    def test_load_from_secrets_merge_order(self):
        """Test that APP_ENV file values override secrets file values."""
        cli = ConcreteCli()
        secrets_data = {
            'APP_ENV': 'test',
            'KEY1': 'secret_value1',
            'KEY2': 'secret_value2',
            'KEY3': 'secret_value3'
        }
        app_env_data = {
            'KEY2': 'app_value2',  # Should override
            'KEY3': 'app_value3',  # Should override
            'KEY4': 'app_value4'   # New key
        }

        def mock_exists(path):
            return path in ['.env.secrets', 'test.env']

        def mock_isfile(path):
            return path in ['.env.secrets', 'test.env']

        def mock_dotenv(path):
            if path == '.env.secrets':
                return secrets_data
            elif path == 'test.env':
                return app_env_data
            return {}

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isfile', side_effect=mock_isfile), \
             patch('rococo.migrations.common.cli_base.dotenv_values', side_effect=mock_dotenv):

            result = cli._load_from_secrets()

            # Verify merge order
            assert result['KEY1'] == 'secret_value1'  # From secrets only
            assert result['KEY2'] == 'app_value2'     # Overridden by app_env
            assert result['KEY3'] == 'app_value3'     # Overridden by app_env
            assert result['KEY4'] == 'app_value4'     # From app_env only


class TestLoadEnv:
    """Test cases for load_env method."""

    def test_load_env_uses_cli_args_when_provided(self):
        """Test that CLI args take precedence when provided."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['test.env'])
        expected_env = OrderedDict([('KEY', 'value')])

        with patch.object(cli, '_load_from_cli_args', return_value=expected_env) as mock_cli_args, \
             patch.object(cli, '_load_from_secrets') as mock_secrets:

            result = cli.load_env(args)

            mock_cli_args.assert_called_once_with(args)
            mock_secrets.assert_not_called()
            assert result == expected_env

    def test_load_env_uses_os_environ_when_all_vars_present(self):
        """Test that os.environ is used when all required vars are present."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=[])

        env_vars = {
            'TEST_HOST': 'localhost',
            'TEST_PORT': '5432',
            'TEST_USER': 'user',
            'TEST_PASSWORD': 'pass'
        }

        with patch('os.getenv', side_effect=lambda k: env_vars.get(k)), \
             patch.object(cli, '_load_from_secrets') as mock_secrets:

            result = cli.load_env(args)

            mock_secrets.assert_not_called()
            assert result['TEST_HOST'] == 'localhost'
            assert result['TEST_PORT'] == '5432'
            assert result['TEST_USER'] == 'user'
            assert result['TEST_PASSWORD'] == 'pass'

    def test_load_env_falls_back_to_secrets_when_vars_missing(self):
        """Test fallback to _load_from_secrets when env vars are missing."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=[])
        expected_env = OrderedDict([('KEY', 'value')])

        # Mock os.getenv to return None (missing vars)
        with patch('os.getenv', return_value=None), \
             patch('builtins.print') as mock_print, \
             patch.object(cli, '_load_from_secrets', return_value=expected_env) as mock_secrets:

            result = cli.load_env(args)

            # Verify print was called with missing vars message
            mock_print.assert_called_once()
            assert 'Missing environment variables' in str(mock_print.call_args)

            # Verify _load_from_secrets was called
            mock_secrets.assert_called_once()
            assert result == expected_env

    def test_load_env_partial_environ_vars_triggers_fallback(self):
        """Test that partial env vars trigger fallback to secrets."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=[])
        expected_env = OrderedDict([('KEY', 'value')])

        # Mock only some required vars present
        def mock_getenv(key):
            if key == 'TEST_HOST':
                return 'localhost'
            return None

        with patch('os.getenv', side_effect=mock_getenv), \
             patch('builtins.print') as mock_print, \
             patch.object(cli, '_load_from_secrets', return_value=expected_env) as mock_secrets:

            result = cli.load_env(args)

            # Verify missing vars are printed
            mock_print.assert_called_once()
            print_call_str = str(mock_print.call_args)
            assert 'TEST_PORT' in print_call_str
            assert 'TEST_USER' in print_call_str
            assert 'TEST_PASSWORD' in print_call_str

            mock_secrets.assert_called_once()
            assert result == expected_env

    def test_load_env_cli_args_take_precedence_over_os_environ(self):
        """Test that CLI args take precedence even when os.environ has all vars."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=['test.env'])
        cli_env = OrderedDict([('FROM', 'cli')])

        env_vars = {
            'TEST_HOST': 'localhost',
            'TEST_PORT': '5432',
            'TEST_USER': 'user',
            'TEST_PASSWORD': 'pass'
        }

        with patch('os.getenv', side_effect=lambda k: env_vars.get(k)), \
             patch.object(cli, '_load_from_cli_args', return_value=cli_env) as mock_cli_args:

            result = cli.load_env(args)

            mock_cli_args.assert_called_once()
            assert result == cli_env  # CLI args win

    def test_load_env_empty_environ_triggers_fallback(self):
        """Test that empty environ triggers fallback with all vars listed as missing."""
        cli = ConcreteCli()
        args = create_mock_args(env_files=[])
        expected_env = OrderedDict([('KEY', 'value')])

        with patch('os.getenv', return_value=None), \
             patch('builtins.print') as mock_print, \
             patch.object(cli, '_load_from_secrets', return_value=expected_env):

            result = cli.load_env(args)
            assert result is not None

            # Verify all required vars are mentioned as missing
            print_call_str = str(mock_print.call_args)
            for var in cli.REQUIRED_ENV_VARS:
                assert var in print_call_str


class TestGetMigrationsDir:
    """Test cases for get_migrations_dir method."""

    def test_get_migrations_dir_uses_provided_path(self):
        """Test that provided migrations_dir is used when it exists."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir='/custom/migrations')

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isdir', return_value=True):

            result = cli.get_migrations_dir(args)

            assert result == '/custom/migrations'

    def test_get_migrations_dir_provided_path_not_exists(self):
        """Test error when provided migrations_dir does not exist."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir='/nonexistent/migrations')

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=False):
            with pytest.raises(ParserErrorException, match="No migrations directory found"):
                cli.get_migrations_dir(args)

    def test_get_migrations_dir_provided_path_is_file(self):
        """Test error when provided migrations_dir is a file."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir='/some/file.txt')

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=True), \
             patch('os.path.isdir', return_value=False):
            with pytest.raises(ParserErrorException, match="No migrations directory found"):
                cli.get_migrations_dir(args)

    def test_get_migrations_dir_uses_first_default(self):
        """Test that first default directory is used when it exists."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir=None)

        def mock_exists(path):
            return path == 'flask/app/migrations'

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isdir', return_value=True):

            result = cli.get_migrations_dir(args)

            assert result == 'flask/app/migrations'

    def test_get_migrations_dir_uses_second_default(self):
        """Test that second default directory is used when first doesn't exist."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir=None)

        def mock_exists(path):
            return path == 'api/app/migrations'

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isdir', return_value=True):

            result = cli.get_migrations_dir(args)

            assert result == 'api/app/migrations'

    def test_get_migrations_dir_uses_third_default(self):
        """Test that third default directory is used when first two don't exist."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir=None)

        def mock_exists(path):
            return path == 'app/migrations'

        with patch('os.path.exists', side_effect=mock_exists), \
             patch('os.path.isdir', return_value=True):

            result = cli.get_migrations_dir(args)

            assert result == 'app/migrations'

    def test_get_migrations_dir_no_valid_path_found(self):
        """Test error when no valid migrations directory is found."""
        cli = ConcreteCli()
        args = create_mock_args(migrations_dir=None)

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch('os.path.exists', return_value=False):
            with pytest.raises(ParserErrorException, match="No migrations directory found"):
                cli.get_migrations_dir(args)


class TestGetMigration:
    """Test cases for get_migration method."""

    def test_get_migration_success(self):
        """Test successful migration object creation."""
        cli = ConcreteCli()
        args = create_mock_args()
        mock_env = {'TEST_HOST': 'localhost', 'TEST_PORT': '5432',
                    'TEST_USER': 'user', 'TEST_PASSWORD': 'pass'}
        mock_adapter = MagicMock()
        mock_migration = MagicMock()

        cli.MIGRATION_CLASS = Mock(return_value=mock_migration)

        with patch.object(cli, 'load_env', return_value=mock_env), \
             patch.object(cli, 'get_db_adapter', return_value=mock_adapter):

            result = cli.get_migration(args)

            cli.MIGRATION_CLASS.assert_called_once_with(mock_adapter)
            assert result == mock_migration

    def test_get_migration_load_env_returns_none(self):
        """Test error when load_env returns None."""
        cli = ConcreteCli()
        args = create_mock_args()

        def mock_error(message):
            raise ParserErrorException(message)

        cli.parser.error = mock_error

        with patch.object(cli, 'load_env', return_value=None):
            with pytest.raises(ParserErrorException, match="Unable to load env"):
                cli.get_migration(args)

    def test_get_migration_calls_get_db_adapter_with_env(self):
        """Test that get_db_adapter is called with correct env."""
        cli = ConcreteCli()
        args = create_mock_args()
        mock_env = {'TEST_HOST': 'localhost'}
        mock_adapter = MagicMock()
        mock_migration_class = Mock(return_value=MagicMock())

        cli.MIGRATION_CLASS = mock_migration_class

        with patch.object(cli, 'load_env', return_value=mock_env), \
             patch.object(cli, 'get_db_adapter', return_value=mock_adapter) as mock_get_adapter:

            cli.get_migration(args)

            mock_get_adapter.assert_called_once_with(mock_env)


class TestRun:
    """Test cases for run method."""

    def test_run_with_new_command(self):
        """Test run method with 'new' command."""
        cli = ConcreteCli()
        args = create_mock_args(command='new')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 5
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print'):

            cli.run()

            mock_path.append.assert_called_once_with('/migrations')
            mock_runner.get_db_version.assert_called_once()
            mock_runner.create_migration_file.assert_called_once()

    def test_run_with_rf_command(self):
        """Test run method with 'rf' command."""
        cli = ConcreteCli()
        args = create_mock_args(command='rf')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 3
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print'):

            cli.run()

            mock_runner.run_forward_migration_script.assert_called_once_with(3)

    def test_run_with_rb_command(self):
        """Test run method with 'rb' command."""
        cli = ConcreteCli()
        args = create_mock_args(command='rb')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 7
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print'):

            cli.run()

            mock_runner.run_backward_migration_script.assert_called_once()

    def test_run_with_version_command(self):
        """Test run method with 'version' command."""
        cli = ConcreteCli()
        args = create_mock_args(command='version')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 10
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print') as mock_print:

            cli.run()

            # Version should be printed twice: once at the start, once for version command
            assert mock_print.call_count == 2
            # Verify no other runner methods were called
            mock_runner.create_migration_file.assert_not_called()
            mock_runner.run_forward_migration_script.assert_not_called()
            mock_runner.run_backward_migration_script.assert_not_called()

    def test_run_with_no_command(self):
        """Test run method with no command."""
        cli = ConcreteCli()
        args = create_mock_args(command=None)
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 2
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli.parser, 'print_help') as mock_print_help, \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print'):

            cli.run()

            mock_print_help.assert_called_once()
            mock_runner.create_migration_file.assert_not_called()
            mock_runner.run_forward_migration_script.assert_not_called()
            mock_runner.run_backward_migration_script.assert_not_called()

    def test_run_appends_migrations_dir_to_sys_path(self):
        """Test that run appends migrations_dir to sys.path."""
        cli = ConcreteCli()
        args = create_mock_args(command='version')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 1
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/custom/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print'):

            cli.run()

            mock_path.append.assert_called_once_with('/custom/migrations')

    def test_run_prints_current_db_version(self):
        """Test that run prints the current DB version."""
        cli = ConcreteCli()
        args = create_mock_args(command='version')
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 42
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=MagicMock()), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner), \
             patch('builtins.print') as mock_print:

            cli.run()

            # Check that DB version 42 was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('42' in call for call in print_calls)

    def test_run_migration_runner_instantiated_correctly(self):
        """Test that MigrationRunner is instantiated with correct parameters."""
        cli = ConcreteCli()
        args = create_mock_args(command='version')
        mock_migration = MagicMock()
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 1
        mock_path = MagicMock(spec=list)

        with patch.object(cli.parser, 'parse_args', return_value=args), \
             patch.object(cli, 'get_migrations_dir', return_value='/test/migrations'), \
             patch.object(sys, 'path', mock_path), \
             patch.object(cli, 'get_migration', return_value=mock_migration), \
             patch('rococo.migrations.common.migration_runner.MigrationRunner', return_value=mock_runner) as mock_runner_class, \
             patch('builtins.print'):

            cli.run()

            mock_runner_class.assert_called_once_with('/test/migrations', mock_migration)
