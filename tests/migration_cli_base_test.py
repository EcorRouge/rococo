"""Tests for rococo.migrations.common.cli_base.BaseCli.

Covers:
- load_env: env-vars-only path, --env-files path, .env.secrets path, missing vars
- get_migrations_dir: explicit path, default fallback, failure
- run flow: dispatches subcommands to MigrationRunner
"""

import argparse
import os
from unittest.mock import MagicMock, patch

import pytest

from rococo.migrations.common.cli_base import BaseCli


class _FakeAdapter:
    pass


class _FakeMigration:
    def __init__(self, adapter):
        self.adapter = adapter


class _ConcreteCli(BaseCli):
    DB_TYPE = "test"
    REQUIRED_ENV_VARS = ["FOO", "BAR"]
    ADAPTER_CLASS = _FakeAdapter
    MIGRATION_CLASS = _FakeMigration

    def get_db_adapter(self, merged_env):
        return _FakeAdapter()


def _cli():
    return _ConcreteCli()


# ---------- load_env ----------

class TestLoadEnv:
    def test_loads_from_environment_when_all_vars_present(self, monkeypatch):
        monkeypatch.setenv("FOO", "fooval")
        monkeypatch.setenv("BAR", "barval")
        cli = _cli()
        args = argparse.Namespace(env_files=[])

        env = cli.load_env(args)

        assert env == {"FOO": "fooval", "BAR": "barval"}

    def test_loads_from_explicit_env_files(self, monkeypatch, tmp_path):
        env_file = tmp_path / "test.env"
        env_file.write_text("FOO=fileval\nBAR=barfile\n")
        cli = _cli()
        args = argparse.Namespace(env_files=[str(env_file)])

        env = cli.load_env(args)

        assert env["FOO"] == "fileval"
        assert env["BAR"] == "barfile"

    def test_explicit_env_file_missing_calls_parser_error(self, tmp_path):
        cli = _cli()
        cli.parser = MagicMock()
        cli.parser.error.side_effect = SystemExit(2)
        args = argparse.Namespace(env_files=[str(tmp_path / "nonexistent.env")])

        with pytest.raises(SystemExit):
            cli.load_env(args)

        cli.parser.error.assert_called_once()

    def test_falls_back_to_dot_env_secrets(self, monkeypatch, tmp_path):
        # Clear required vars so it goes into the .env.secrets fallback
        monkeypatch.delenv("FOO", raising=False)
        monkeypatch.delenv("BAR", raising=False)
        monkeypatch.chdir(tmp_path)

        (tmp_path / ".env.secrets").write_text("APP_ENV=staging\nFOO=secret\n")
        (tmp_path / "staging.env").write_text("BAR=barvalue\n")

        cli = _cli()
        args = argparse.Namespace(env_files=[])

        env = cli.load_env(args)
        assert env["FOO"] == "secret"
        assert env["BAR"] == "barvalue"

    def test_missing_dot_env_secrets_calls_parser_error(self, monkeypatch, tmp_path):
        monkeypatch.delenv("FOO", raising=False)
        monkeypatch.delenv("BAR", raising=False)
        monkeypatch.chdir(tmp_path)

        cli = _cli()
        cli.parser = MagicMock()
        cli.parser.error.side_effect = SystemExit(2)
        args = argparse.Namespace(env_files=[])

        with pytest.raises(SystemExit):
            cli.load_env(args)

    def test_dot_env_secrets_without_app_env_calls_parser_error(self, monkeypatch, tmp_path):
        monkeypatch.delenv("FOO", raising=False)
        monkeypatch.delenv("BAR", raising=False)
        monkeypatch.chdir(tmp_path)

        # .env.secrets exists but lacks APP_ENV
        (tmp_path / ".env.secrets").write_text("FOO=val\n")

        cli = _cli()
        cli.parser = MagicMock()
        cli.parser.error.side_effect = SystemExit(2)
        args = argparse.Namespace(env_files=[])

        with pytest.raises(SystemExit):
            cli.load_env(args)


# ---------- get_migrations_dir ----------

class TestGetMigrationsDir:
    def test_uses_explicit_path_when_valid(self, tmp_path):
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        cli = _cli()
        args = argparse.Namespace(migrations_dir=str(migrations))

        result = cli.get_migrations_dir(args)
        assert result == str(migrations)

    def test_falls_back_to_default_locations(self, monkeypatch, tmp_path):
        # Create one of the default fallback dirs
        (tmp_path / "app" / "migrations").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        cli = _cli()
        args = argparse.Namespace(migrations_dir=None)

        result = cli.get_migrations_dir(args)
        assert result == "app/migrations"

    def test_calls_parser_error_when_nothing_found(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        cli = _cli()
        cli.parser = MagicMock()
        cli.parser.error.side_effect = SystemExit(2)
        args = argparse.Namespace(migrations_dir=None)

        with pytest.raises(SystemExit):
            cli.get_migrations_dir(args)

    def test_invalid_explicit_path_falls_through_to_defaults(self, monkeypatch, tmp_path):
        # Create a default fallback so the call succeeds via the fallback path
        (tmp_path / "flask" / "app" / "migrations").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)
        cli = _cli()
        args = argparse.Namespace(migrations_dir="/nonexistent/path")

        result = cli.get_migrations_dir(args)
        assert result == "flask/app/migrations"


# ---------- run flow ----------

class TestRunDispatch:
    def _run_with_command(self, command, monkeypatch, tmp_path):
        (tmp_path / "app" / "migrations").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("FOO", "fooval")
        monkeypatch.setenv("BAR", "barval")

        cli = _cli()
        # Stub argparse to return our chosen command
        cli.parser = MagicMock()
        cli.parser.parse_args.return_value = argparse.Namespace(
            command=command,
            env_files=[],
            migrations_dir=None,
        )

        with patch(
            "rococo.migrations.common.migration_runner.MigrationRunner"
        ) as RunnerClass:
            runner = RunnerClass.return_value
            runner.get_db_version.return_value = "0000000003"
            cli.run()
            return RunnerClass, runner

    def test_run_dispatches_rf(self, monkeypatch, tmp_path):
        RunnerClass, runner = self._run_with_command("rf", monkeypatch, tmp_path)
        runner.run_forward_migration_script.assert_called_once_with("0000000003")
        runner.run_backward_migration_script.assert_not_called()
        runner.create_migration_file.assert_not_called()

    def test_run_dispatches_rb(self, monkeypatch, tmp_path):
        RunnerClass, runner = self._run_with_command("rb", monkeypatch, tmp_path)
        runner.run_backward_migration_script.assert_called_once_with()
        runner.run_forward_migration_script.assert_not_called()

    def test_run_dispatches_new(self, monkeypatch, tmp_path):
        RunnerClass, runner = self._run_with_command("new", monkeypatch, tmp_path)
        runner.create_migration_file.assert_called_once_with()

    def test_run_dispatches_version(self, monkeypatch, tmp_path, capsys):
        RunnerClass, runner = self._run_with_command("version", monkeypatch, tmp_path)
        runner.run_forward_migration_script.assert_not_called()
        runner.run_backward_migration_script.assert_not_called()
        runner.create_migration_file.assert_not_called()

    def test_run_unknown_command_prints_help(self, monkeypatch, tmp_path):
        (tmp_path / "app" / "migrations").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("FOO", "fooval")
        monkeypatch.setenv("BAR", "barval")

        cli = _cli()
        cli.parser = MagicMock()
        cli.parser.parse_args.return_value = argparse.Namespace(
            command=None,
            env_files=[],
            migrations_dir=None,
        )

        with patch(
            "rococo.migrations.common.migration_runner.MigrationRunner"
        ) as RunnerClass:
            runner = RunnerClass.return_value
            runner.get_db_version.return_value = "0000000000"
            cli.run()

        cli.parser.print_help.assert_called_once()
