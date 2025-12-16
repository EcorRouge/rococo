"""
Unit tests for rococo/migrations/common/cli_base.py
"""
import unittest
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock
from collections import OrderedDict

from rococo.migrations.common.cli_base import BaseCli


class MockMigration:
    """Mock migration class for testing."""
    def __init__(self, *args, **kwargs):
        pass


class MockAdapter:
    """Mock database adapter for testing."""
    pass


class ConcreteCli(BaseCli):
    """Concrete implementation of BaseCli for testing."""
    DB_TYPE = 'test'
    REQUIRED_ENV_VARS = ['DB_HOST', 'DB_PORT', 'DB_NAME']
    ADAPTER_CLASS = MockAdapter
    MIGRATION_CLASS = MockMigration
    
    def get_db_adapter(self, merged_env):
        return MockAdapter()


class TestBaseCliCreateParser(unittest.TestCase):
    """Tests for _create_parser method."""

    def test_parser_created(self):
        """Test that parser is created successfully."""
        cli = ConcreteCli()
        self.assertIsNotNone(cli.parser)

    def test_parser_has_migrations_dir_argument(self):
        """Test that parser has --migrations-dir argument."""
        cli = ConcreteCli()
        args = cli.parser.parse_args(['--migrations-dir', '/path/to/migrations', 'version'])
        self.assertEqual(args.migrations_dir, '/path/to/migrations')

    def test_parser_has_env_files_argument(self):
        """Test that parser has --env-files argument."""
        cli = ConcreteCli()
        # Note: nargs='+' means all following positional args are included
        # So we need to use -- to separate, or parse without a subcommand
        args = cli.parser.parse_args(['--env-files', 'file1.env', 'file2.env', '--', 'version'])
        self.assertIn('file1.env', args.env_files)
        self.assertIn('file2.env', args.env_files)

    def test_parser_has_subcommands(self):
        """Test that parser has all subcommands."""
        cli = ConcreteCli()
        
        for cmd in ['new', 'rf', 'rb', 'version']:
            args = cli.parser.parse_args([cmd])
            self.assertEqual(args.command, cmd)


class TestBaseCliLoadEnv(unittest.TestCase):
    """Tests for load_env method."""

    def setUp(self):
        self.cli = ConcreteCli()
        # Clear relevant env vars
        for var in ['DB_HOST', 'DB_PORT', 'DB_NAME']:
            if var in os.environ:
                del os.environ[var]

    def tearDown(self):
        # Clean up env vars
        for var in ['DB_HOST', 'DB_PORT', 'DB_NAME']:
            if var in os.environ:
                del os.environ[var]

    def test_load_env_from_environment(self):
        """Test loading env from environment variables."""
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '5432'
        os.environ['DB_NAME'] = 'testdb'
        
        args = MagicMock()
        args.env_files = []
        
        result = self.cli.load_env(args)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['DB_HOST'], 'localhost')
        self.assertEqual(result['DB_PORT'], '5432')
        self.assertEqual(result['DB_NAME'], 'testdb')

    def test_load_env_from_env_files(self):
        """Test loading env from specified env files."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write('DB_HOST=filehost\nDB_PORT=3306\nDB_NAME=filedb')
            env_file_path = f.name
        
        try:
            args = MagicMock()
            args.env_files = [env_file_path]
            
            result = self.cli.load_env(args)
            
            self.assertIsNotNone(result)
            self.assertEqual(result['DB_HOST'], 'filehost')
        finally:
            os.unlink(env_file_path)

    def test_load_env_missing_env_file(self):
        """Test load_env with non-existent env file."""
        args = MagicMock()
        args.env_files = ['/nonexistent/path/to.env']
        
        with patch.object(self.cli.parser, 'error') as mock_error:
            self.cli.load_env(args)
            mock_error.assert_called()

    def test_load_env_partial_env_vars(self):
        """Test load_env when some required vars are in environment."""
        os.environ['DB_HOST'] = 'localhost'
        # DB_PORT and DB_NAME are missing
        
        args = MagicMock()
        args.env_files = []
        
        with patch.object(self.cli.parser, 'error') as mock_error:
            self.cli.load_env(args)
            # Should try to load from .env files since vars are missing


class TestBaseCliLoadEnvSecretsFile(unittest.TestCase):
    """Tests for load_env with .env.secrets file."""

    def setUp(self):
        self.cli = ConcreteCli()
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp()
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.original_cwd)
        # Clean up temp files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_env_from_secrets_file(self):
        """Test loading env from .env.secrets file."""
        # Create .env.secrets file
        with open('.env.secrets', 'w') as f:
            f.write('APP_ENV=development\nDB_HOST=secrethost')
        
        # Create development.env file
        with open('development.env', 'w') as f:
            f.write('DB_PORT=5432\nDB_NAME=devdb')
        
        args = MagicMock()
        args.env_files = []
        
        result = self.cli.load_env(args)
        
        # Should merge secrets and app env
        self.assertIsNotNone(result)

    def test_load_env_missing_app_env(self):
        """Test load_env when APP_ENV file is missing."""
        # Create .env.secrets file without corresponding env file
        with open('.env.secrets', 'w') as f:
            f.write('APP_ENV=nonexistent')
        
        args = MagicMock()
        args.env_files = []
        
        with patch.object(self.cli.parser, 'error') as mock_error:
            self.cli.load_env(args)
            # Should error because nonexistent.env doesn't exist


class TestBaseCliGetMigrationsDir(unittest.TestCase):
    """Tests for get_migrations_dir method."""

    def setUp(self):
        self.cli = ConcreteCli()
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp()
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_migrations_dir_with_provided_path(self):
        """Test get_migrations_dir with valid provided path."""
        migrations_dir = os.path.join(self.temp_dir, 'custom_migrations')
        os.makedirs(migrations_dir)
        
        args = MagicMock()
        args.migrations_dir = migrations_dir
        
        result = self.cli.get_migrations_dir(args)
        
        self.assertEqual(result, migrations_dir)

    def test_get_migrations_dir_default_flask(self):
        """Test get_migrations_dir with default flask/app/migrations path."""
        flask_migrations = os.path.join(self.temp_dir, 'flask', 'app', 'migrations')
        os.makedirs(flask_migrations)
        
        args = MagicMock()
        args.migrations_dir = None
        
        result = self.cli.get_migrations_dir(args)
        
        self.assertEqual(result, 'flask/app/migrations')

    def test_get_migrations_dir_default_api(self):
        """Test get_migrations_dir with default api/app/migrations path."""
        api_migrations = os.path.join(self.temp_dir, 'api', 'app', 'migrations')
        os.makedirs(api_migrations)
        
        args = MagicMock()
        args.migrations_dir = None
        
        result = self.cli.get_migrations_dir(args)
        
        self.assertEqual(result, 'api/app/migrations')

    def test_get_migrations_dir_default_app(self):
        """Test get_migrations_dir with default app/migrations path."""
        app_migrations = os.path.join(self.temp_dir, 'app', 'migrations')
        os.makedirs(app_migrations)
        
        args = MagicMock()
        args.migrations_dir = None
        
        result = self.cli.get_migrations_dir(args)
        
        self.assertEqual(result, 'app/migrations')

    def test_get_migrations_dir_not_found(self):
        """Test get_migrations_dir when no directory is found."""
        args = MagicMock()
        args.migrations_dir = None
        
        with patch.object(self.cli.parser, 'error') as mock_error:
            self.cli.get_migrations_dir(args)
            mock_error.assert_called_with("No migrations directory found.")


class TestBaseCliGetMigration(unittest.TestCase):
    """Tests for get_migration method."""

    def setUp(self):
        self.cli = ConcreteCli()

    def test_get_migration_success(self):
        """Test get_migration returns migration instance."""
        args = MagicMock()
        
        with patch.object(self.cli, 'load_env') as mock_load_env:
            mock_load_env.return_value = {'DB_HOST': 'localhost'}
            
            result = self.cli.get_migration(args)
            
            self.assertIsInstance(result, MockMigration)

    def test_get_migration_load_env_fails(self):
        """Test get_migration when load_env returns None."""
        args = MagicMock()
        
        with patch.object(self.cli, 'load_env') as mock_load_env:
            mock_load_env.return_value = None
            
            with patch.object(self.cli.parser, 'error') as mock_error:
                self.cli.get_migration(args)
                mock_error.assert_called_with("Unable to load env.")


class TestBaseCliRun(unittest.TestCase):
    """Tests for run method."""

    def setUp(self):
        self.cli = ConcreteCli()
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp()
        os.chdir(self.temp_dir)
        
        # Create migrations directory
        os.makedirs('app/migrations')

    def tearDown(self):
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('rococo.migrations.common.cli_base.BaseCli.get_migration')
    @patch('rococo.migrations.common.migration_runner.MigrationRunner')
    def test_run_version_command(self, mock_runner_class, mock_get_migration):
        """Test run with version command."""
        mock_migration = MagicMock()
        mock_get_migration.return_value = mock_migration
        
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 5
        mock_runner_class.return_value = mock_runner
        
        with patch.object(sys, 'argv', ['cli', 'version']):
            self.cli.run()
            
            mock_runner.get_db_version.assert_called()

    @patch('rococo.migrations.common.cli_base.BaseCli.get_migration')
    @patch('rococo.migrations.common.migration_runner.MigrationRunner')
    def test_run_new_command(self, mock_runner_class, mock_get_migration):
        """Test run with new command."""
        mock_migration = MagicMock()
        mock_get_migration.return_value = mock_migration
        
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 1
        mock_runner_class.return_value = mock_runner
        
        with patch.object(sys, 'argv', ['cli', 'new']):
            self.cli.run()
            
            mock_runner.create_migration_file.assert_called()

    @patch('rococo.migrations.common.cli_base.BaseCli.get_migration')
    @patch('rococo.migrations.common.migration_runner.MigrationRunner')
    def test_run_rf_command(self, mock_runner_class, mock_get_migration):
        """Test run with rf (run forward) command."""
        mock_migration = MagicMock()
        mock_get_migration.return_value = mock_migration
        
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 3
        mock_runner_class.return_value = mock_runner
        
        with patch.object(sys, 'argv', ['cli', 'rf']):
            self.cli.run()
            
            mock_runner.run_forward_migration_script.assert_called_with(3)

    @patch('rococo.migrations.common.cli_base.BaseCli.get_migration')
    @patch('rococo.migrations.common.migration_runner.MigrationRunner')
    def test_run_rb_command(self, mock_runner_class, mock_get_migration):
        """Test run with rb (run backward) command."""
        mock_migration = MagicMock()
        mock_get_migration.return_value = mock_migration
        
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 5
        mock_runner_class.return_value = mock_runner
        
        with patch.object(sys, 'argv', ['cli', 'rb']):
            self.cli.run()
            
            mock_runner.run_backward_migration_script.assert_called()

    @patch('rococo.migrations.common.cli_base.BaseCli.get_migration')
    @patch('rococo.migrations.common.migration_runner.MigrationRunner')
    def test_run_no_command(self, mock_runner_class, mock_get_migration):
        """Test run without any command shows help."""
        mock_migration = MagicMock()
        mock_get_migration.return_value = mock_migration
        
        mock_runner = MagicMock()
        mock_runner.get_db_version.return_value = 1
        mock_runner_class.return_value = mock_runner
        
        with patch.object(sys, 'argv', ['cli']):
            with patch.object(self.cli.parser, 'print_help') as mock_help:
                self.cli.run()
                mock_help.assert_called()


if __name__ == '__main__':
    unittest.main()
