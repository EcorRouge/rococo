# Rococo Models Guide

## Overview

Rococo provides two base model types for different use cases:

### BaseModel - The Unversioned Model

**BaseModel is the unversioned model.** Use it for data that doesn't need audit trails or version history.

**Use Cases**:
- Configuration settings
- Static reference data
- Cache entries
- Temporary data
- Logs that don't need versioning
- Simple lookup tables
- Session data

**Fields**:
- `entity_id`: Unique identifier (UUID hex string, auto-generated)
- `extra`: Dictionary for additional fields (stored but not exposed in standard serialization)

**Example**:
```python
from rococo.models import BaseModel, NonVersionedModel
from dataclasses import dataclass

@dataclass(kw_only=True)
class Config(BaseModel):  # or NonVersionedModel
    key: str
    value: str

# Create a config entry
config = Config(key="api_key", value="secret123")
config.prepare_for_save()
# Save to database - no version tracking, no audit trail
```

### VersionedModel - Versioned with Audit Trail

**VersionedModel extends BaseModel** to add full versioning and audit trail capabilities.

**Use Cases**:
- User profiles
- Organizations
- Sensitive business data
- Data requiring compliance tracking
- Multi-user edited content
- Financial records
- Medical records
- Legal documents

**Fields** (Big 6 + BaseModel fields):
- `entity_id`: Unique identifier (UUID hex string)
- `version`: Current version UUID
- `previous_version`: Previous version UUID (for history chain)
- `active`: Active status (soft delete flag)
- `changed_by_id`: UUID of user who made the change
- `changed_on`: Timestamp of change
- `extra`: Dictionary for additional fields

**Example**:
```python
from rococo.models import VersionedModel
from dataclasses import dataclass

@dataclass(kw_only=True)
class Person(VersionedModel):
    first_name: str
    last_name: str
    email: str

# Create a person
person = Person(first_name="John", last_name="Doe", email="john@example.com")
person.prepare_for_save(changed_by_id="user123")
# Save to database - creates audit entry

# Update creates new version
person.first_name = "Jane"
person.prepare_for_save(changed_by_id="user123")
# Previous version moved to audit table
```

## Choosing Between BaseModel and VersionedModel

| Factor | BaseModel (Unversioned) | VersionedModel (Versioned) |
|--------|------------------------|---------------------------|
| Audit trail needed? | ❌ No | ✅ Yes |
| Version history? | ❌ No | ✅ Yes |
| Soft delete? | ❌ No | ✅ Yes |
| Track who changed? | ❌ No | ✅ Yes |
| Track when changed? | ❌ No | ✅ Yes |

## Decision Guide

### Use BaseModel (Unversioned) When:
- ✅ Data is temporary or disposable
- ✅ No need to track who/when changed
- ✅ No need for version history
- ✅ Performance is critical
- ✅ Storage space is constrained
- ✅ Data is read-heavy, write-light

### Use VersionedModel When:
- ✅ Need audit trail for compliance
- ✅ Multiple users editing data
- ✅ Need to recover previous versions
- ✅ Legal/regulatory requirements
- ✅ Business-critical data
- ✅ Need soft delete capability

## NonVersionedModel Alias

`NonVersionedModel` is an alias for `BaseModel` (the unversioned model). They are exactly the same thing - use whichever name makes more sense in your context:

```python
from rococo.models import NonVersionedModel, BaseModel

# These are identical:
NonVersionedModel == BaseModel  # True

# Use either name:
@dataclass(kw_only=True)
class Config(BaseModel):          # Preferred (base of model hierarchy)
    key: str

@dataclass(kw_only=True)
class Setting(NonVersionedModel):  # Also works (emphasizes it's unversioned)
    value: str
```

**Why two names?**
- `BaseModel` - Technical name (it's the base of the model hierarchy)
- `NonVersionedModel` - Descriptive name (emphasizes it's unversioned)

Both refer to the same unversioned model class.

## Field Details

### BaseModel Fields

#### `entity_id`
- **Type**: `str` (UUID hex string)
- **Default**: Auto-generated on creation
- **Purpose**: Unique identifier for the entity across all versions
- **Example**: `"a1b2c3d4e5f6789012345678901234567"`

#### `extra`
- **Type**: `Dict[str, Any]`
- **Default**: `{}`
- **Purpose**: Storage for additional fields not in model definition
- **Behavior**: Contents are flattened into serialization output (not nested)
- **Example**:
  ```python
  config = Config(key="api_key", value="secret")
  config.extra = {'created_by': 'admin', 'environment': 'prod'}

  # Serialize - extra contents are flattened
  data = config.as_dict()
  # data = {'entity_id': '...', 'key': 'api_key', 'value': 'secret',
  #         'created_by': 'admin', 'environment': 'prod'}
  ```

### VersionedModel Additional Fields (Big 6)

#### `version`
- **Type**: `str` (UUID hex string)
- **Default**: Auto-generated on first save
- **Purpose**: Unique identifier for this specific version

#### `previous_version`
- **Type**: `str` (UUID hex string or None)
- **Default**: `None`
- **Purpose**: Links to previous version for history chain

#### `active`
- **Type**: `bool`
- **Default**: `True`
- **Purpose**: Soft delete flag (False = deleted)

#### `changed_by_id`
- **Type**: `str` (UUID hex string)
- **Default**: Must be provided on save
- **Purpose**: Tracks who made the change

#### `changed_on`
- **Type**: `datetime`
- **Default**: Auto-generated on save
- **Purpose**: Timestamp of when change was made

## Common Operations

### Creating Instances

```python
# BaseModel (unversioned)
config = Config(key="timeout", value="30")

# VersionedModel (versioned)
person = Person(first_name="John", last_name="Doe")
```

### Preparing for Save

```python
# BaseModel - no parameters needed
config.prepare_for_save()

# VersionedModel - requires changed_by_id
person.prepare_for_save(changed_by_id="user-uuid")
```

### Serialization

```python
# Convert to dictionary
data = model.as_dict()

# For database storage
db_data = model.get_for_db()

# For API responses
api_data = model.get_for_api()
```

### Deserialization

```python
# From dictionary
model = Config.from_dict({'key': 'api_key', 'value': 'secret'})
```

### Validation

```python
# Validate all fields
model.validate()  # Raises ModelValidationError if invalid
```

## Working with Repositories

Both model types work with Rococo repositories:

```python
from rococo.repositories.surrealdb import SurrealDbRepository
from rococo.data.surrealdb import SurrealDbAdapter

# Setup adapter and repository
adapter = SurrealDbAdapter(...)
with adapter:
    repo = SurrealDbRepository(adapter, message_adapter=None)

    # Save BaseModel (unversioned)
    repo.save(Config, config, "config_table")

    # Save VersionedModel (versioned - creates audit entry)
    repo.save(Person, person, "person_table", changed_by_id="user123")

    # Get one
    config = repo.get_one(Config, "config_table", {"key": "api_key"})

    # Get many
    people = repo.get_many(Person, "person_table", {"active": True})
```

## Migration Guide

### From VersionedModel to BaseModel
**Not recommended** - you'll lose audit history and version tracking.

If you must:
1. Export existing data
2. Change model inheritance from VersionedModel to BaseModel
3. Remove Big 6 field usage from code
4. Migrate data to new table structure

### From BaseModel to VersionedModel
**Safe to do** - backward compatible:

```python
# Before
@dataclass(kw_only=True)
class MyModel(BaseModel):
    field: str

# After
@dataclass(kw_only=True)
class MyModel(VersionedModel):
    field: str
```

**Migration steps**:
1. Change parent class from BaseModel to VersionedModel
2. Update `prepare_for_save()` calls to include `changed_by_id`
3. Update `save()` calls to include `changed_by_id` parameter
4. Big 6 fields will be auto-populated on save
5. Existing data will get versioning fields on next update

## Best Practices

### BaseModel (Unversioned)

1. **Use for ephemeral data**:
   ```python
   @dataclass(kw_only=True)
   class CacheEntry(BaseModel):
       cache_key: str
       cache_value: dict
       ttl: int
   ```

2. **Configuration management**:
   ```python
   @dataclass(kw_only=True)
   class AppConfig(BaseModel):
       key: str
       value: str
       environment: str
   ```

3. **Logs without history**:
   ```python
   @dataclass(kw_only=True)
   class LogEntry(BaseModel):
       level: str
       message: str
       timestamp: datetime
   ```

### VersionedModel

1. **Always provide changed_by_id**:
   ```python
   person.prepare_for_save(changed_by_id=current_user_id)
   ```

2. **Use soft delete instead of hard delete**:
   ```python
   # Don't delete from database
   # Instead, mark as inactive
   person.active = False
   person.prepare_for_save(changed_by_id=current_user_id)
   repo.save(Person, person, "person_table", changed_by_id=current_user_id)
   ```

3. **Query only active records**:
   ```python
   active_people = repo.get_many(Person, "person_table", {"active": True})
   ```

## Advanced Topics

### Custom Validation

```python
@dataclass(kw_only=True)
class Person(VersionedModel):
    email: str
    age: int

    def validate_email(self):
        """Validate email format."""
        if '@' not in self.email:
            return "Invalid email format"

    def validate_age(self):
        """Validate age is positive."""
        if self.age < 0:
            return "Age must be positive"

# Validation runs on validate() call
person = Person(email="invalid", age=-5)
person.validate()  # Raises ModelValidationError with both errors
```

### Using Extra Fields

```python
# Store arbitrary data in extra field
config = Config(key="api_settings", value="{}")
config.extra = {
    'created_by': 'admin',
    'created_at': datetime.now(),
    'tags': ['production', 'critical']
}

# Extra contents are flattened in serialization
data = config.as_dict()
# data includes: created_by, created_at, tags at top level
```

### Relationships

```python
@dataclass(kw_only=True)
class Organization(VersionedModel):
    name: str

@dataclass(kw_only=True)
class Person(VersionedModel):
    first_name: str
    organization_id: str = field(metadata={'field_type': 'entity_id'})
```

## Troubleshooting

### Issue: prepare_for_save() fails with "changed_by_id required"
**Solution**: You're using VersionedModel. Always provide changed_by_id:
```python
model.prepare_for_save(changed_by_id="user-uuid")
```

### Issue: Extra field data not appearing in from_dict()
**Solution**: This is expected behavior. Extra fields are flattened in as_dict() but not collected back in from_dict() unless the model has `allow_extra=True`.

### Issue: Version field is None after creation
**Solution**: Version is only populated after `prepare_for_save()` is called:
```python
person = Person(first_name="John", last_name="Doe")
person.prepare_for_save(changed_by_id="user123")  # Now version is set
```

## Summary

- **BaseModel**: Unversioned model for simple data without audit trails
- **VersionedModel**: Extends BaseModel with versioning and audit trail (Big 6 fields)
- **NonVersionedModel**: Alias for BaseModel (same class, different name)
- Choose based on whether you need audit trails and version history
- Both work seamlessly with Rococo repositories and adapters

For more information, see:
- [Repository Documentation](repositories.md)
- [Database Adapters](adapters.md)
- [API Reference](api.md)
