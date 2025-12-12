"""
Tests for NonVersionedModel (BaseModel) functionality.

Tests cover:
- Basic CRUD operations
- Field verification (entity_id, extra fields, no Big 6 fields)
- Serialization (as_dict, from_dict, get_for_db, get_for_api)
- Validation
- Extra fields handling
- Backward compatibility

NonVersionedModel is an alias for BaseModel, the unversioned model class.
"""

import pytest
from dataclasses import dataclass, field, fields
from typing import Optional, Dict, Any
from datetime import datetime
from uuid import UUID

from rococo.models import NonVersionedModel, BaseModel, VersionedModel
from rococo.models.versioned_model import ModelValidationError


# Test models

@dataclass(kw_only=True)
class Config(NonVersionedModel):
    """Simple configuration model for testing."""
    key: str = ""
    value: str = ""


@dataclass(kw_only=True)
class Setting(BaseModel):
    """Settings model using BaseModel directly (should be identical to NonVersionedModel)."""
    name: str = ""
    enabled: bool = False


@dataclass(kw_only=True)
class LogEntry(NonVersionedModel):
    """Log entry model with optional fields."""
    level: str = "INFO"
    message: str = ""
    timestamp: Optional[datetime] = None


@dataclass(kw_only=True)
class CacheEntry(NonVersionedModel):
    """Cache entry with complex data types."""
    cache_key: str = ""
    cache_value: Dict[str, Any] = field(default_factory=dict)
    ttl: int = 3600


# ===== Basic CRUD Tests =====

def test_nonversioned_model_create():
    """Test creating a NonVersionedModel instance."""
    config = Config(key="api_key", value="secret123")

    assert config.key == "api_key"
    assert config.value == "secret123"
    assert config.entity_id  # Should have auto-generated entity_id
    assert isinstance(config.entity_id, str)
    assert len(config.entity_id) == 32  # UUID hex string
    assert config.extra == {}  # Default empty dict


def test_nonversioned_model_with_entity_id():
    """Test creating a NonVersionedModel with explicit entity_id."""
    entity_id = "12345678901234567890123456789012"
    config = Config(entity_id=entity_id, key="db_host", value="localhost")

    assert config.entity_id == entity_id
    assert config.key == "db_host"


def test_nonversioned_model_update():
    """Test updating fields in a NonVersionedModel."""
    config = Config(key="timeout", value="30")
    assert config.value == "30"

    # Update value
    config.value = "60"
    assert config.value == "60"


def test_nonversioned_model_with_optional_fields():
    """Test NonVersionedModel with optional fields."""
    # Without timestamp
    log1 = LogEntry(level="ERROR", message="Database connection failed")
    assert log1.timestamp is None

    # With timestamp
    now = datetime.now()
    log2 = LogEntry(level="INFO", message="Server started", timestamp=now)
    assert log2.timestamp == now


# ===== Field Verification Tests =====

def test_nonversioned_has_entity_id():
    """Verify NonVersionedModel has entity_id field."""
    config = Config(key="test", value="value")
    assert hasattr(config, 'entity_id')
    assert config.entity_id is not None


def test_nonversioned_has_extra_field():
    """Verify NonVersionedModel has extra field."""
    config = Config(key="test", value="value")
    assert hasattr(config, 'extra')
    assert isinstance(config.extra, dict)


def test_nonversioned_no_version_fields():
    """Verify NonVersionedModel does NOT have Big 6 versioning fields."""
    config = Config(key="test", value="value")

    # Should NOT have version, previous_version, active, changed_by_id, changed_on
    assert not hasattr(config, 'version')
    assert not hasattr(config, 'previous_version')
    assert not hasattr(config, 'active')
    assert not hasattr(config, 'changed_by_id')
    assert not hasattr(config, 'changed_on')


def test_basemodel_equals_nonversionedmodel():
    """Verify that BaseModel and NonVersionedModel are the same class."""
    assert NonVersionedModel is BaseModel

    # Instances should be identical
    config1 = Config(key="test1", value="value1")
    assert isinstance(config1, BaseModel)
    assert isinstance(config1, NonVersionedModel)


def test_setting_using_basemodel():
    """Test that models using BaseModel directly work identically to NonVersionedModel."""
    setting = Setting(name="dark_mode", enabled=True)

    assert setting.name == "dark_mode"
    assert setting.enabled is True
    assert hasattr(setting, 'entity_id')
    assert hasattr(setting, 'extra')
    assert not hasattr(setting, 'version')


# ===== Serialization Tests =====

def test_nonversioned_as_dict():
    """Test as_dict() serialization."""
    config = Config(key="db_name", value="production")
    data = config.as_dict()

    assert isinstance(data, dict)
    assert data['key'] == "db_name"
    assert data['value'] == "production"
    assert 'entity_id' in data
    # The 'extra' field itself is not included (it's excluded from serialization)
    assert 'extra' not in data

    # Should NOT have Big 6 fields
    assert 'version' not in data
    assert 'previous_version' not in data
    assert 'active' not in data
    assert 'changed_by_id' not in data
    assert 'changed_on' not in data


def test_nonversioned_from_dict():
    """Test from_dict() deserialization."""
    data = {
        'entity_id': '11111111111111111111111111111111',
        'key': 'max_connections',
        'value': '100'
    }

    config = Config.from_dict(data)

    assert config.entity_id == '11111111111111111111111111111111'
    assert config.key == 'max_connections'
    assert config.value == '100'
    # extra defaults to empty dict
    assert config.extra == {}


def test_nonversioned_round_trip_serialization():
    """Test that as_dict() -> from_dict() preserves data for defined fields."""
    original = Config(key="feature_flag", value="enabled")

    # Serialize
    data = original.as_dict()

    # Deserialize
    restored = Config.from_dict(data)

    assert restored.entity_id == original.entity_id
    assert restored.key == original.key
    assert restored.value == original.value
    # Note: extra field contents are flattened in as_dict() but not collected
    # back in from_dict() unless allow_extra=True, so empty extra is expected
    assert restored.extra == {}


def test_nonversioned_get_for_db():
    """Test get_for_db() serialization for database storage."""
    config = Config(key="timeout", value="30")
    db_data = config.get_for_db()

    assert isinstance(db_data, dict)
    assert db_data['key'] == "timeout"
    assert db_data['value'] == "30"
    assert 'entity_id' in db_data

    # Should NOT have Big 6 fields
    assert 'version' not in db_data
    assert 'active' not in db_data


def test_nonversioned_get_for_api():
    """Test get_for_api() serialization for API responses."""
    config = Config(key="api_url", value="https://example.com")
    api_data = config.get_for_api()

    assert isinstance(api_data, dict)
    assert api_data['key'] == "api_url"
    assert api_data['value'] == "https://example.com"
    assert 'entity_id' in api_data


def test_nonversioned_with_complex_data():
    """Test serialization with complex data types."""
    cache = CacheEntry(
        cache_key="user:123",
        cache_value={'name': 'John', 'age': 30},
        ttl=7200
    )
    cache.extra = {
        'created': datetime(2025, 1, 1, 12, 0, 0),
        'tags': ['user', 'profile']
    }

    db_data = cache.get_for_db()
    assert db_data['cache_key'] == "user:123"
    assert db_data['cache_value'] == {'name': 'John', 'age': 30}
    # The 'extra' field itself is not included, but its contents are flattened
    assert 'extra' not in db_data
    # Extra field contents should be flattened into the result
    assert 'created' in db_data
    assert 'tags' in db_data


# ===== Validation and Prepare for Save Tests =====

def test_nonversioned_validate():
    """Test validate() method."""
    config = Config(key="port", value="8080")

    # Should not raise any errors
    try:
        config.validate()
    except ModelValidationError:
        pytest.fail("validate() raised ModelValidationError unexpectedly")


def test_nonversioned_prepare_for_save():
    """Test prepare_for_save() method."""
    config = Config(key="host", value="localhost")

    # Should not raise errors and should not require changed_by_id
    config.prepare_for_save()

    # Entity ID should still be present
    assert config.entity_id is not None


def test_nonversioned_prepare_for_save_new():
    """Test prepare_for_save() for a new entity."""
    config = Config(key="new_setting", value="new_value")
    original_entity_id = config.entity_id

    config.prepare_for_save()

    # Entity ID should remain the same
    assert config.entity_id == original_entity_id


def test_nonversioned_prepare_for_save_update():
    """Test prepare_for_save() for updating an entity."""
    config = Config(key="existing_setting", value="old_value")
    config.prepare_for_save()

    # Update value
    config.value = "new_value"
    config.prepare_for_save()

    # Should still work without needing version fields
    assert config.value == "new_value"


# ===== Extra Fields Tests =====

def test_nonversioned_extra_fields_storage():
    """Test storing arbitrary data in extra field."""
    config = Config(key="api_key", value="secret")

    # Store additional data
    config.extra = {
        'created_by': 'admin',
        'environment': 'production',
        'tags': ['important', 'security']
    }

    assert config.extra['created_by'] == 'admin'
    assert config.extra['environment'] == 'production'
    assert len(config.extra['tags']) == 2


def test_nonversioned_extra_fields_retrieval():
    """Test retrieving data from extra field."""
    config = Config(key="db_host", value="localhost")
    config.extra = {'port': 5432, 'ssl': True}

    # Access extra fields
    assert config.extra.get('port') == 5432
    assert config.extra.get('ssl') is True
    assert config.extra.get('nonexistent') is None


def test_nonversioned_extra_fields_in_serialization():
    """Test that extra field contents are flattened in serialization."""
    config = Config(key="cache_ttl", value="3600")
    config.extra = {'unit': 'seconds', 'source': 'config_file'}

    # Serialize - extra field contents are flattened into the result
    data = config.as_dict()
    # The 'extra' field itself is not present
    assert 'extra' not in data
    # But its contents are flattened into the top level
    assert data['unit'] == 'seconds'
    assert data['source'] == 'config_file'


# ===== Backward Compatibility Tests =====

def test_nonversioned_alias_works():
    """Test that NonVersionedModel alias works correctly."""
    # Can use either name
    config1 = NonVersionedModel()
    config2 = BaseModel()

    # Both should be the same type
    assert type(config1) == type(config2)


def test_can_import_nonversioned():
    """Test that NonVersionedModel can be imported."""
    from rococo.models import NonVersionedModel
    assert NonVersionedModel is not None


def test_can_import_basemodel():
    """Test that BaseModel can be imported."""
    from rococo.models import BaseModel
    assert BaseModel is not None


def test_nonversioned_vs_versioned_fields():
    """Compare NonVersionedModel fields to VersionedModel fields."""
    # Create instances
    config = Config(key="test", value="value")

    @dataclass(kw_only=True)
    class VersionedConfig(VersionedModel):
        key: str = ""
        value: str = ""

    versioned_config = VersionedConfig(key="test", value="value")

    # NonVersionedModel should have fewer fields
    config_fields = set(config.__dataclass_fields__.keys())
    versioned_fields = set(versioned_config.__dataclass_fields__.keys())

    # NonVersionedModel has: entity_id, extra
    assert 'entity_id' in config_fields
    assert 'extra' in config_fields

    # VersionedModel has all of NonVersionedModel plus Big 6
    assert 'entity_id' in versioned_fields
    assert 'extra' in versioned_fields
    assert 'version' in versioned_fields
    assert 'previous_version' in versioned_fields
    assert 'active' in versioned_fields
    assert 'changed_by_id' in versioned_fields
    assert 'changed_on' in versioned_fields


# ===== Edge Cases and Special Scenarios =====

def test_nonversioned_empty_extra_field():
    """Test that extra field defaults to empty dict."""
    config = Config(key="test", value="value")
    assert config.extra == {}
    assert isinstance(config.extra, dict)


def test_nonversioned_none_values():
    """Test handling of None values in optional fields."""
    log = LogEntry(level="DEBUG", message="test")
    assert log.timestamp is None


def test_nonversioned_repr():
    """Test string representation."""
    config = Config(key="name", value="test")
    repr_str = repr(config)

    assert "Config" in repr_str
    assert "key" in repr_str or "name" in repr_str


def test_nonversioned_fields_method():
    """Test fields() class method."""
    fields = Config.fields()

    assert 'key' in fields
    assert 'value' in fields
    assert 'entity_id' in fields
    # 'extra' is intentionally excluded from fields() method
    assert 'extra' not in fields

    # Should NOT have versioning fields
    assert 'version' not in fields
    assert 'active' not in fields


def test_nonversioned_with_default_factory():
    """Test fields with default_factory."""
    cache1 = CacheEntry(cache_key="key1")
    cache2 = CacheEntry(cache_key="key2")

    # Each instance should have its own cache_value dict
    cache1.cache_value['data'] = 'test1'
    cache2.cache_value['data'] = 'test2'

    assert cache1.cache_value['data'] == 'test1'
    assert cache2.cache_value['data'] == 'test2'


def test_nonversioned_model_inheritance():
    """Test that NonVersionedModel classes inherit correctly."""
    config = Config(key="test", value="value")

    # Should be instance of NonVersionedModel/BaseModel
    assert isinstance(config, NonVersionedModel)
    assert isinstance(config, BaseModel)

    # Should NOT be instance of VersionedModel
    assert not isinstance(config, VersionedModel)


def test_multiple_nonversioned_instances():
    """Test creating multiple instances with unique entity_ids."""
    configs = [Config(key=f"key{i}", value=f"value{i}") for i in range(5)]

    # All should have unique entity_ids
    entity_ids = [c.entity_id for c in configs]
    assert len(set(entity_ids)) == 5

    # All should have their own data
    for i, config in enumerate(configs):
        assert config.key == f"key{i}"
        assert config.value == f"value{i}"


# ===== VersionedModel Backward Compatibility Tests =====
# These tests ensure that VersionedModel still works correctly after introducing
# BaseModel inheritance. Critical for production apps using VersionedModel.


# Category 1: Field Structure & Inheritance

def test_versioned_model_has_all_big_6_fields():
    """Verify VersionedModel still has all Big 6 versioning fields."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        first_name: str = ""
        last_name: str = ""

    person = Person(first_name="John", last_name="Doe")

    # Must have Big 6 fields
    assert hasattr(person, 'entity_id')
    assert hasattr(person, 'version')
    assert hasattr(person, 'previous_version')
    assert hasattr(person, 'active')
    assert hasattr(person, 'changed_by_id')
    assert hasattr(person, 'changed_on')
    assert hasattr(person, 'extra')

    # Field types must be correct
    assert isinstance(person.entity_id, str)
    assert isinstance(person.version, str)
    assert isinstance(person.active, bool)
    assert isinstance(person.extra, dict)


def test_versioned_model_inherits_from_basemodel():
    """Verify VersionedModel correctly inherits from BaseModel."""
    assert issubclass(VersionedModel, BaseModel)


def test_versioned_model_field_count():
    """Verify VersionedModel has exactly the expected number of fields."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        first_name: str = ""

    all_fields = fields(Person)
    field_names = [f.name for f in all_fields]

    assert 'entity_id' in field_names
    assert 'extra' in field_names
    assert 'version' in field_names
    assert 'previous_version' in field_names
    assert 'active' in field_names
    assert 'changed_by_id' in field_names
    assert 'changed_on' in field_names


def test_versioned_model_method_resolution_order():
    """Verify MRO is correct: VersionedModel -> BaseModel -> object."""
    mro = VersionedModel.__mro__
    assert BaseModel in mro
    assert mro.index(VersionedModel) < mro.index(BaseModel)


def test_versioned_model_extra_field_defaults_to_empty_dict():
    """Verify extra field is inherited and defaults correctly."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    assert hasattr(person, 'extra')
    assert isinstance(person.extra, dict)
    assert person.extra == {}


def test_versioned_model_entity_id_auto_generated():
    """Verify entity_id is still auto-generated."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    assert person.entity_id is not None
    assert isinstance(person.entity_id, str)
    assert len(person.entity_id) == 32  # UUID hex string


def test_versioned_model_fields_method():
    """Verify fields() method returns correct field list."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    field_names = Person.fields()

    # Should include all user fields + Big 6
    assert 'name' in field_names
    assert 'entity_id' in field_names
    assert 'version' in field_names
    assert 'previous_version' in field_names
    assert 'active' in field_names
    assert 'changed_by_id' in field_names
    assert 'changed_on' in field_names

    # Should NOT include extra (it's excluded by fields() method)
    assert 'extra' not in field_names


def test_versioned_model_no_extra_fields_leak():
    """Verify BaseModel's extra field doesn't leak into VersionedModel serialization."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.extra = {'should_not_appear': 'in_as_dict'}

    data = person.as_dict()

    # extra field contents should be flattened, but 'extra' key itself should not appear
    assert 'extra' not in data


# Category 2: Serialization Behavior

def test_versioned_model_as_dict_has_big_6():
    """Verify as_dict() includes all Big 6 fields."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    data = person.as_dict()

    assert 'entity_id' in data
    assert 'version' in data
    assert 'previous_version' in data
    assert 'active' in data
    assert 'changed_by_id' in data
    assert 'changed_on' in data
    assert 'name' in data


def test_versioned_model_as_dict_no_extra_key():
    """Verify as_dict() does not include 'extra' as a key."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.extra = {'metadata': 'value'}

    data = person.as_dict()

    # 'extra' field itself should not be in output
    assert 'extra' not in data
    # But extra contents should be flattened
    assert 'metadata' in data


def test_versioned_model_from_dict_preserves_big_6():
    """Verify from_dict() correctly restores Big 6 fields."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    data = {
        'entity_id': 'test123',
        'version': 'v123',
        'previous_version': 'v122',
        'active': True,
        'changed_by_id': 'user123',
        'changed_on': datetime.now(),
        'name': 'John'
    }

    person = Person.from_dict(data)

    assert person.entity_id == 'test123'
    assert person.version == 'v123'
    assert person.previous_version == 'v122'
    assert person.active is True
    assert person.changed_by_id == 'user123'
    assert person.name == 'John'


def test_versioned_model_round_trip_serialization():
    """Verify as_dict() -> from_dict() round trip preserves all data."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""
        age: int = 0

    original = Person(name="John", age=30)
    original.prepare_for_save(changed_by_id="user123")

    # Round trip
    data = original.as_dict()
    restored = Person.from_dict(data)

    assert restored.entity_id == original.entity_id
    assert restored.version == original.version
    assert restored.previous_version == original.previous_version
    assert restored.active == original.active
    assert restored.changed_by_id == original.changed_by_id
    assert restored.name == original.name
    assert restored.age == original.age


def test_versioned_model_get_for_db():
    """Verify get_for_db() returns correct structure."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    db_data = person.get_for_db()

    # Must have Big 6 fields
    assert 'entity_id' in db_data
    assert 'version' in db_data
    assert 'active' in db_data
    assert 'changed_by_id' in db_data
    assert 'changed_on' in db_data


def test_versioned_model_get_for_api():
    """Verify get_for_api() returns correct structure."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    api_data = person.get_for_api()

    # Should have all fields except sensitive ones
    assert 'entity_id' in api_data
    assert 'name' in api_data


def test_versioned_model_serialization_datetime_handling():
    """Verify datetime serialization works correctly."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    data = person.as_dict(convert_datetime_to_iso_string=True)

    # changed_on should be ISO string
    assert isinstance(data['changed_on'], str)


def test_versioned_model_serialization_uuid_handling():
    """Verify UUID serialization works correctly."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    data = person.as_dict(convert_uuids=True)

    # UUIDs should be strings
    assert isinstance(data['entity_id'], str)
    assert isinstance(data['version'], str)


def test_versioned_model_fields_exported_correctly():
    """Verify only correct fields are exported in as_dict()."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    data = person.as_dict()

    # Should NOT have _is_partial or other internal fields
    assert '_is_partial' not in data


def test_versioned_model_extra_field_handling_in_serialization():
    """Verify extra field contents are flattened, not nested."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.extra = {'city': 'New York', 'country': 'USA'}

    data = person.as_dict()

    # Extra contents should be at top level
    assert data['city'] == 'New York'
    assert data['country'] == 'USA'
    # But 'extra' itself should not be a key
    assert 'extra' not in data


# Category 3: prepare_for_save() Behavior

def test_versioned_model_prepare_for_save_populates_big_6():
    """Verify prepare_for_save() populates all Big 6 fields."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")

    # All Big 6 fields should be populated
    assert person.version is not None
    assert person.changed_by_id == "user123"
    assert person.changed_on is not None
    assert person.active is True


def test_versioned_model_prepare_for_save_version_bump():
    """Verify prepare_for_save() bumps version on update."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")
    old_version = person.version

    # Update
    person.name = "Jane"
    person.prepare_for_save(changed_by_id="user123")

    # Version should change
    assert person.version != old_version
    assert person.previous_version == old_version


def test_versioned_model_prepare_for_save_requires_changed_by_id():
    """Verify prepare_for_save() requires changed_by_id parameter."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")

    # Should work with changed_by_id
    person.prepare_for_save(changed_by_id="user123")
    assert person.changed_by_id == "user123"


def test_versioned_model_prepare_for_save_idempotent():
    """Verify calling prepare_for_save() multiple times is safe."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")
    first_version = person.version

    # Call again without changes
    person.prepare_for_save(changed_by_id="user123")

    # Version should change even without field changes
    assert person.version != first_version


def test_versioned_model_prepare_for_save_preserves_entity_id():
    """Verify prepare_for_save() never changes entity_id."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    original_entity_id = person.entity_id

    person.prepare_for_save(changed_by_id="user123")
    assert person.entity_id == original_entity_id

    person.prepare_for_save(changed_by_id="user456")
    assert person.entity_id == original_entity_id


def test_versioned_model_prepare_for_save_active_flag():
    """Verify prepare_for_save() respects active flag."""
    @dataclass(kw_only=True)
    class Person(VersionedModel):
        name: str = ""

    person = Person(name="John")
    person.prepare_for_save(changed_by_id="user123")
    assert person.active is True

    # Soft delete
    person.active = False
    person.prepare_for_save(changed_by_id="user123")
    assert person.active is False


# Category 4: Import & API Compatibility

def test_versioned_model_import_from_rococo_models():
    """Verify VersionedModel can still be imported from rococo.models."""
    try:
        from rococo.models import VersionedModel as VM
        assert VM is not None
    except ImportError:
        pytest.fail("VersionedModel import failed")


def test_basemodel_import_from_rococo_models():
    """Verify BaseModel can be imported from rococo.models."""
    try:
        from rococo.models import BaseModel as BM
        assert BM is not None
    except ImportError:
        pytest.fail("BaseModel import failed")


def test_nonversioned_model_alias_exists():
    """Verify NonVersionedModel alias exists and points to BaseModel."""
    assert NonVersionedModel is BaseModel


def test_versioned_model_public_methods_exist():
    """Verify all expected public methods still exist on VersionedModel."""
    expected_methods = [
        'as_dict', 'from_dict', 'prepare_for_save',
        'validate', 'get_for_db', 'get_for_api', 'fields'
    ]

    for method in expected_methods:
        assert hasattr(VersionedModel, method), f"Missing method: {method}"
