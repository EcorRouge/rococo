"""
Tests for the Config class from the config module
"""
import os
import pytest
from rococo.config import Config

@pytest.fixture
def env_setup():
    os.environ["VAR_1"] = "value1"
    os.environ["VAR_2"] = "value2"
    os.environ["TO_LIST_VAR"] = "A,B,C"
    os.environ["JSON_STRING"] = '{"some_key":"some_value"}'
    yield
    del os.environ["VAR_1"]
    del os.environ["VAR_2"]
    del os.environ["TO_LIST_VAR"]
    del os.environ["JSON_STRING"]

def test_create_config(env_setup):
    config = Config()
    assert config.get_env_var("VAR_1") == "value1"
    assert config.get_env_var("VAR_2") == "value2"
    assert config.get_env_var("TO_LIST_VAR") == "A,B,C"

def test_var_to_list(env_setup):
    config = Config()
    assert config.convert_var_into_list("TO_LIST_VAR") is True
    assert config.get_env_var("TO_LIST_VAR") == ["A","B","C"]

def test_var_from_json(env_setup):
    config = Config()
    assert config.convert_var_from_json_string("JSON_STRING") is True
    assert config.get_env_var("JSON_STRING") == {"some_key":"some_value"}

def test_project_toml_version(env_setup):
    config = Config()
    try:
        with open("pyproject.toml","w",encoding="UTF-8") as f:
            f.write('version = "1.0.0"')
        project_root = os.path.dirname(os.path.abspath(__file__))
        while not os.path.exists(os.path.join(project_root, 'pyproject.toml')):
            project_root = os.path.dirname(project_root)
            assert config.load_toml(project_root) is True
            assert config.get_project_version() == "1.0.0"
    finally:
        os.remove("pyproject.toml")
    
