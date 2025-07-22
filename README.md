# rococo

A Python library to help build things the way we want them built.

_Anything worth doing is worth doing well.  Anything worth doing twice is worth doing in rococo._

[Decision Log](https://ecorrouge.github.io/rococo) - [![Rococo Decisions](https://ecorrouge.github.io/rococo/badge.svg)](https://ecorrouge.github.io/rococo)  
[How to document new decision](https://github.com/EcorRouge/rococo/tree/main/docs/decision-log-overview.md#how-to-add-new-decision)  

## Table of Contents

- [Basic Usage](#basic-usage)
  - [Installation](#installation)
  - [Example](#example)
- [Models](#models)
  - [Enum Field Conversion](#enum-field-conversion)
  - [Dataclass Field Conversion](#dataclass-field-conversion)
  - [Extra Fields Support](#extra-fields-support)
    - [Direct Field Access](#direct-field-access)
  - [Field Aliases](#field-aliases)
    - [Basic Alias Usage](#basic-alias-usage)
    - [Alias Restrictions](#alias-restrictions)
    - [Complex Type Integration](#complex-type-integration)
    - [Roundtrip Consistency](#roundtrip-consistency)
  - [Calculated Properties](#calculated-properties)
    - [Basic Property Usage](#basic-property-usage)
    - [Repository Integration](#repository-integration)
    - [Advanced Property Features](#advanced-property-features)
    - [Property Inheritance](#property-inheritance)
    - [API Response Usage](#api-response-usage)
- [Messaging](#messaging)
  - [RabbitMQ](#rabbitmq)
  - [SQS](#sqs)
  - [Processing](#processing)
- [Data](#data)
  - [SurrealDB](#surrealdb)
  - [PostgreSQL](#postgresql)
  - [Relationships in Surreal DB](#relationships-in-surreal-db)
    - [Many-to-many relationships](#many-to-many-relationships)
  - [Relationships in MySQL](#relationships-in-mysql)
  - [Relationships in PostgreSQL](#relationships-in-postgresql-1)
- [Repository Usage](#repository-usage)
  - [How to use the adapter and base Repository in another projects](#how-to-use-the-adapter-and-base-repository-in-another-projects)
  - [Repository Configuration Options](#repository-configuration-options)
    - [Auditing Control](#auditing-control)
    - [TTL (Time To Live) Fields for MongoDB](#ttl-time-to-live-fields-for-mongodb)
    - [Calculated Fields Control](#calculated-fields-control)
  - [RepositoryFactory](#repositoryfactory)
  - [Sample usage](#sample-usage)
- [CLI Tools](#cli-tools)
  - [Rococo MySQL CLI (`rococo-mysql`)](#rococo-mysql-cli-rococo-mysql)
    - [Usage](#usage)
    - [Options](#options)
    - [Commands](#commands)
    - [Environment Configuration](#environment-configuration)
    - [Example](#example-1)
- [Email Integration](#email-integration)
  - [Using `email-transmitter` in your project](#using-email-transmitter-in-your-project)
- [Deployment](#deployment)
  - [Development Phase](#development-phase)
  - [Staging/Testing Phase](#stagingtesting-phase)
  - [Release/Publish Phase](#releasepublish-phase)
- [Local Development](#local-development)

## Basic Usage

### Installation

Install using pip:

```bash
pip install rococo
```

### Example

#### Models

```python
from rococo.models import Person

# Initialize a Person object from rococo's built-in models.
someone = Person(first_name="John", last_name="Doe")

# Prepare to save the object in the database adding/updating attributes for the object.
someone.prepare_for_save(changed_by_id=UUID("b30884cb-5127-457c-a633-4a800ad3c44b"))

someone.as_dict()
```

OUTPUT:

```python
{
    'active': True,
    'changed_by_id': 'b30884cb-5127-457c-a633-4a800ad3c44b',
    'changed_on': datetime.datetime(2023, 9, 20, 19, 50, 23, 532875),
    'entity_id': 'ba68b3b1-fccd-4035-92f6-0ac2b29d71a1',
    'first_name': 'John',
    'last_name': 'Doe',
    'previous_version': '3261fc4d-7db4-4945-91b5-9fb6a4b7dbc5',
    'version': '4b6d92de-64bc-4dfb-a824-2151e8f11b73'
}
```

#### Enum Field Conversion

VersionedModel automatically converts enum fields to and from their string values when using `as_dict()` and `from_dict()`:

```python
from enum import Enum
from typing import Optional
from dataclasses import dataclass
from rococo.models import VersionedModel

class OrganizationImportStatus(Enum):
    uploading = "uploading"
    canceled = "canceled"
    pending = "pending"

@dataclass
class OrganizationImport(VersionedModel):
    status: Optional[OrganizationImportStatus] = OrganizationImportStatus.pending

# Enum to dict conversion
model = OrganizationImport(status=OrganizationImportStatus.uploading)
result = model.as_dict()
print(result['status'])  # Output: "uploading" (string)

# Dict to enum conversion
data = {"status": "canceled"}
restored = OrganizationImport.from_dict(data)
print(restored.status)  # Output: OrganizationImportStatus.canceled (enum)

# Roundtrip conversion maintains consistency
original = OrganizationImport(status=OrganizationImportStatus.uploading)
dict_data = original.as_dict()
restored = OrganizationImport.from_dict(dict_data)
assert restored.status == original.status  # True
```

#### Dataclass Field Conversion

VersionedModel can automatically convert dataclass fields to and from dictionaries using the `metadata={'model': DataclassType}` field specification:

```python
from dataclasses import dataclass, field
from typing import Optional, List
from rococo.models import VersionedModel

@dataclass
class OrganizationImportError:
    message: Optional[str] = None
    code: Optional[int] = None

@dataclass
class OrganizationImport(VersionedModel):
    # Single dataclass field with metadata
    runtime_error: Optional[OrganizationImportError] = field(
        default=None, metadata={'model': OrganizationImportError}
    )
    # List of dataclass fields with metadata
    errors: Optional[List[OrganizationImportError]] = field(
        default_factory=list, metadata={'model': OrganizationImportError}
    )

# Dataclass to dict conversion
error = OrganizationImportError(message="Test error", code=500)
errors = [OrganizationImportError(message="Error 1", code=400)]
model = OrganizationImport(runtime_error=error, errors=errors)

result = model.as_dict()
print(result['runtime_error'])  # Output: {'message': 'Test error', 'code': 500}
print(result['errors'])         # Output: [{'message': 'Error 1', 'code': 400}]

# Dict to dataclass conversion
data = {
    'runtime_error': {'message': 'Restored error', 'code': 404},
    'errors': [{'message': 'List error', 'code': 422}]
}
restored = OrganizationImport.from_dict(data)

print(type(restored.runtime_error))  # Output: <class 'OrganizationImportError'>
print(restored.runtime_error.message)  # Output: "Restored error"
print(len(restored.errors))  # Output: 1
print(restored.errors[0].message)  # Output: "List error"

# Roundtrip conversion maintains consistency
original = OrganizationImport(
    runtime_error=OrganizationImportError(message="Original", code=200),
    errors=[OrganizationImportError(message="Original error", code=400)]
)
dict_data = original.as_dict()
restored = OrganizationImport.from_dict(dict_data)

assert restored.runtime_error.message == original.runtime_error.message  # True
assert restored.errors[0].code == original.errors[0].code  # True
```

**Note**: Only fields with `metadata={'model': DataclassType}` are automatically converted. Fields without this metadata remain unchanged during conversion.

#### Extra Fields Support

VersionedModel supports "extra" fields similar to Pydantic, allowing models to accept and store additional fields that are not explicitly defined in the model schema:

```python
from dataclasses import dataclass
from rococo.models import VersionedModel

# Model with extra fields enabled
@dataclass
class ModelWithExtra(VersionedModel):
    allow_extra = True  # Enable extra fields support
    name: str = "test"

# Load model with extra fields from dict
data = {
    "name": "test_model",
    "custom_field": "custom_value",
    "dynamic_config": {"setting": "value"},
    "score": 95.5
}

model = ModelWithExtra.from_dict(data)
print(model.name)  # Output: "test_model"
print(model.extra)  # Output: {"custom_field": "custom_value", "dynamic_config": {...}, "score": 95.5}

# Convert back to dict - extra fields are unwrapped
result = model.as_dict()
print(result["name"])           # Output: "test_model"
print(result["custom_field"])   # Output: "custom_value"
print(result["score"])          # Output: 95.5
print("extra" in result)        # Output: False (extra field itself is not included)

# Model without extra fields (default behavior)
@dataclass
class ModelWithoutExtra(VersionedModel):
    name: str = "test"

model_no_extra = ModelWithoutExtra.from_dict(data)
print(model_no_extra.name)   # Output: "test_model"
print(model_no_extra.extra)  # Output: {} (extra fields are ignored)

# Roundtrip conversion maintains consistency
original = ModelWithExtra(name="original")
original.extra = {"dynamic_field": "dynamic_value", "score": 95.5}

dict_data = original.as_dict()
restored = ModelWithExtra.from_dict(dict_data)

assert restored.name == original.name                    # True
assert restored.extra == original.extra                  # True
assert restored.extra["dynamic_field"] == "dynamic_value"  # True
```

##### Direct Field Access

Extra fields can be accessed directly as attributes on the model instance, providing a seamless experience similar to regular model fields:

```python
from dataclasses import dataclass
from rococo.models import VersionedModel

@dataclass
class ModelWithExtra(VersionedModel):
    allow_extra = True
    name: str = "test"

# Load model with extra fields
data = {
    "name": "test_model",
    "custom_field": "custom_value",
    "score": 95.5,
    "config": {"theme": "dark", "notifications": True}
}

model = ModelWithExtra.from_dict(data)

# Direct attribute access to extra fields
print(model.name)           # Output: "test_model" (regular field)
print(model.custom_field)   # Output: "custom_value" (extra field)
print(model.score)          # Output: 95.5 (extra field)
print(model.config)         # Output: {"theme": "dark", "notifications": True} (extra field)

# Setting extra fields directly as attributes
model.dynamic_field = "dynamic_value"
model.priority = 10

print(model.dynamic_field)  # Output: "dynamic_value"
print(model.priority)       # Output: 10

# Extra fields are automatically included in the extra dict
print(model.extra)
# Output: {
#     "custom_field": "custom_value",
#     "score": 95.5,
#     "config": {"theme": "dark", "notifications": True},
#     "dynamic_field": "dynamic_value",
#     "priority": 10
# }

# Direct access works seamlessly with as_dict()
result = model.as_dict()
print(result["custom_field"])   # Output: "custom_value"
print(result["dynamic_field"])  # Output: "dynamic_value"
print("extra" in result)        # Output: False (extra dict is unwrapped)
```

**Key Features:**
- **Configurable**: Set `allow_extra = True` to enable extra fields support
- **Transparent Storage**: Extra fields are stored in the `extra` dict attribute
- **Direct Access**: Access extra fields directly as attributes (e.g., `model.custom_field`)
- **Dynamic Assignment**: Set extra fields directly as attributes (e.g., `model.new_field = value`)
- **Database Friendly**: Extra fields are unwrapped in `as_dict()` for seamless database storage
- **Silent Ignoring**: When `allow_extra = False` (default), extra fields are silently ignored
- **Roundtrip Consistency**: Extra fields maintain consistency through save/load cycles
- **Integration**: Works alongside enum and dataclass conversion features

#### Field Aliases

VersionedModel supports field aliases that allow different property naming during serialization and deserialization. This is useful for API compatibility, database schema differences, or when you need different field names in your application versus external systems.

##### Basic Alias Usage

```python
from dataclasses import dataclass, field
from rococo.models import VersionedModel

@dataclass
class User(VersionedModel):
    # Use aliases for external API compatibility
    full_name: str = field(default="", metadata={'alias': 'name'})
    email_address: str = field(default="", metadata={'alias': 'email'})
    user_score: int = field(default=0, metadata={'alias': 'rating'})
    # Field without alias uses original name
    description: str = "No description"

user = User(
    full_name="John Doe",
    email_address="john@example.com", 
    user_score=95,
    description="Test user"
)

# Serialization uses aliases
result = user.as_dict()
print(result)
# Output: {
#     "name": "John Doe",           # aliased from full_name
#     "email": "john@example.com",  # aliased from email_address  
#     "rating": 95,                 # aliased from user_score
#     "description": "Test user",   # no alias, original name used
#     "entity_id": "...",           # Big 6 fields always use original names
#     "version": "...",
#     "active": True,
#     # ... other Big 6 fields
# }

# Deserialization handles aliases
api_data = {
    "name": "Jane Smith",
    "email": "jane@example.com", 
    "rating": 88,
    "description": "API user",
    "entity_id": "test-id"
}

restored_user = User.from_dict(api_data)
print(restored_user.full_name)      # Output: "Jane Smith"
print(restored_user.email_address)  # Output: "jane@example.com"
print(restored_user.user_score)     # Output: 88
```

##### Alias Restrictions

Field aliases have important restrictions to maintain data integrity:

```python
from dataclasses import dataclass, field
from rococo.models import VersionedModel

@dataclass
class RestrictedAliasModel(VersionedModel):
    # ❌ Big 6 fields cannot use aliases (these will be ignored)
    entity_id: str = field(default_factory=get_uuid_hex, metadata={'alias': 'id'})
    version: str = field(default_factory=get_uuid_hex, metadata={'alias': 'ver'})
    active: bool = field(default=True, metadata={'alias': 'is_active'})
    
    # ✅ Custom fields can use aliases
    custom_field: str = field(default="test", metadata={'alias': 'custom_name'})

model = RestrictedAliasModel(custom_field="test value")
result = model.as_dict()

# Big 6 fields always use original names (aliases ignored)
assert "entity_id" in result  # ✅ Original name used
assert "version" in result    # ✅ Original name used  
assert "active" in result     # ✅ Original name used
assert "id" not in result     # ❌ Alias ignored
assert "ver" not in result    # ❌ Alias ignored
assert "is_active" not in result  # ❌ Alias ignored

# Custom fields use aliases
assert "custom_name" in result    # ✅ Alias used
assert "custom_field" not in result  # ❌ Original name not used
```

**Big 6 Fields (No Aliases Allowed):**
- `entity_id`
- `version` 
- `previous_version`
- `changed_on`
- `changed_by_id`
- `active`

##### Complex Type Integration

Field aliases work seamlessly with other VersionedModel features:

```python
from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum
from rococo.models import VersionedModel

class Priority(Enum):
    low = "low"
    medium = "medium" 
    high = "high"

@dataclass
class TaskDetails:
    estimated_hours: int = 0
    complexity: str = "simple"

@dataclass
class Task(VersionedModel):
    allow_extra = True  # Enable extra fields
    
    # Aliased fields with complex types
    task_title: str = field(default="", metadata={'alias': 'title'})
    task_priority: Priority = field(default=Priority.medium, metadata={'alias': 'priority'})
    task_details: Optional[TaskDetails] = field(
        default=None, 
        metadata={'alias': 'details', 'model': TaskDetails}
    )
    assigned_users: List[str] = field(default_factory=list, metadata={'alias': 'assignees'})
    
    @property
    def display_name(self) -> str:
        """Computed property (not aliased)."""
        return f"[{self.task_priority.value.upper()}] {self.task_title}"

# Create task with complex data
task = Task(
    task_title="Implement feature X",
    task_priority=Priority.high,
    task_details=TaskDetails(estimated_hours=8, complexity="complex"),
    assigned_users=["user1", "user2"]
)

# Add extra field
task.custom_metadata = {"source": "api", "version": "2.0"}

# Serialization with aliases
result = task.as_dict()
print(result)
# Output: {
#     "title": "Implement feature X",        # aliased from task_title
#     "priority": "high",                    # aliased from task_priority (enum → string)
#     "details": {                           # aliased from task_details (dataclass → dict)
#         "estimated_hours": 8,
#         "complexity": "complex"
#     },
#     "assignees": ["user1", "user2"],       # aliased from assigned_users
#     "custom_metadata": {                   # extra field (no alias)
#         "source": "api", 
#         "version": "2.0"
#     },
#     "display_name": "[HIGH] Implement feature X",  # computed property (no alias)
#     "entity_id": "...",                    # Big 6 field (no alias)
#     # ... other fields
# }

# Deserialization with aliases
api_data = {
    "title": "Updated task",
    "priority": "low", 
    "details": {"estimated_hours": 4, "complexity": "simple"},
    "assignees": ["user3"],
    "project_id": "proj-123"  # extra field
}

restored_task = Task.from_dict(api_data)
print(restored_task.task_title)     # Output: "Updated task"
print(restored_task.task_priority)  # Output: Priority.low
print(restored_task.task_details.estimated_hours)  # Output: 4
print(restored_task.assigned_users) # Output: ["user3"]
print(restored_task.extra["project_id"])  # Output: "proj-123"
```

##### Roundtrip Consistency

Field aliases maintain perfect consistency through serialization and deserialization cycles:

```python
from dataclasses import dataclass, field
from rococo.models import VersionedModel

@dataclass
class Product(VersionedModel):
    product_name: str = field(default="", metadata={'alias': 'name'})
    product_price: float = field(default=0.0, metadata={'alias': 'price'})
    product_category: str = field(default="", metadata={'alias': 'category'})

# Create original product
original = Product(
    product_name="Laptop",
    product_price=999.99,
    product_category="Electronics"
)

# Serialize using aliases
serialized_data = original.as_dict()
print("Serialized:", serialized_data)
# Output: {
#     "name": "Laptop",
#     "price": 999.99, 
#     "category": "Electronics",
#     "entity_id": "...",
#     # ... other fields
# }

# Deserialize from aliased data
restored = Product.from_dict(serialized_data)

# Verify perfect roundtrip consistency
assert restored.product_name == original.product_name        # ✅ "Laptop"
assert restored.product_price == original.product_price      # ✅ 999.99
assert restored.product_category == original.product_category # ✅ "Electronics"
assert restored.entity_id == original.entity_id             # ✅ Same ID

# Serialize again to verify consistency
re_serialized = restored.as_dict()
assert serialized_data == re_serialized  # ✅ Perfect roundtrip consistency

print("Roundtrip successful! ✅")
```

**Key Features:**
- **Flexible Naming**: Use different field names for internal models vs external APIs
- **Big 6 Protection**: Core VersionedModel fields always use original names for data integrity
- **Complex Type Support**: Works with enums, dataclasses, extra fields, and computed properties
- **Roundtrip Consistency**: Perfect data integrity through serialization/deserialization cycles
- **Backward Compatibility**: Models without aliases work exactly as before
- **Mixed Data Support**: Handles data containing both aliased and original field names
- **Error Resilience**: Invalid or empty aliases are gracefully ignored

#### Calculated Properties

VersionedModel supports calculated properties (Python `@property` decorators) that can be included or excluded from serialization. This is particularly useful for computed fields, API responses, and derived data that shouldn't be stored in the database.

##### Basic Property Usage

```python
from dataclasses import dataclass
from rococo.models import VersionedModel

@dataclass
class User(VersionedModel):
    first_name: str = "John"
    last_name: str = "Doe"
    email: str = "john.doe@example.com"
    
    @property
    def full_name(self) -> str:
        """Computed property combining first and last name."""
        return f"{self.first_name} {self.last_name}"
    
    @property
    def display_info(self) -> dict:
        """Complex property returning structured data."""
        return {
            "name": self.full_name,
            "contact": self.email,
            "initials": f"{self.first_name[0]}{self.last_name[0]}"
        }

user = User(first_name="Jane", last_name="Smith", email="jane@example.com")

# Properties are included by default in as_dict()
result = user.as_dict()
print(result["full_name"])      # Output: "Jane Smith"
print(result["display_info"])   # Output: {"name": "Jane Smith", "contact": "jane@example.com", "initials": "JS"}

# Properties can be excluded using export_properties=False
db_data = user.as_dict(export_properties=False)
print("full_name" in db_data)   # Output: False
print("display_info" in db_data) # Output: False
```

##### Repository Integration

By default, repositories exclude calculated properties when saving to the database to prevent storing computed values. This behavior can be configured:

```python
from rococo.repositories import BaseRepository
from rococo.data import PostgreSQLAdapter

# Default behavior: properties excluded from database saves
repository = BaseRepository(
    db_adapter=adapter,
    model=User,
    message_adapter=message_adapter,
    queue_name="user_queue"
)

user = User(first_name="Alice", last_name="Johnson")

# When saving, properties are automatically excluded
repository.save(user)  # Only stores: first_name, last_name, email, entity_id, version, etc.

# Configure repository to include calculated fields in database saves
repository.save_calculated_fields = True
repository.save(user)  # Now also stores: full_name, display_info

# Or configure during initialization
repository_with_properties = BaseRepository(
    db_adapter=adapter,
    model=User,
    message_adapter=message_adapter,
    queue_name="user_queue",
    save_calculated_fields=True  # Include properties in database saves
)
```

##### Advanced Property Features

```python
from dataclasses import dataclass
from typing import Optional
from rococo.models import VersionedModel

@dataclass
class Product(VersionedModel):
    name: str = "Sample Product"
    price: float = 0.0
    tax_rate: float = 0.1
    category: str = "general"
    
    @property
    def price_with_tax(self) -> float:
        """Calculate price including tax."""
        return self.price * (1 + self.tax_rate)
    
    @property
    def category_info(self) -> dict:
        """Return category metadata."""
        categories = {
            "electronics": {"priority": "high", "warranty": "2 years"},
            "clothing": {"priority": "medium", "warranty": "30 days"},
            "general": {"priority": "low", "warranty": "1 year"}
        }
        return categories.get(self.category, categories["general"])
    
    @property
    def is_expensive(self) -> bool:
        """Determine if product is considered expensive."""
        return self.price_with_tax > 100.0
    
    @property
    def error_property(self) -> str:
        """Property that raises an exception (handled gracefully)."""
        if self.price < 0:
            raise ValueError("Price cannot be negative")
        return "Valid price"

product = Product(name="Laptop", price=899.99, category="electronics")

# All properties are computed and included
result = product.as_dict()
print(result["price_with_tax"])  # Output: 989.989 (899.99 * 1.1)
print(result["category_info"])   # Output: {"priority": "high", "warranty": "2 years"}
print(result["is_expensive"])    # Output: True
print(result["error_property"])  # Output: "Valid price"

# Properties with exceptions are handled gracefully
broken_product = Product(name="Broken", price=-10.0)
result = broken_product.as_dict()
print("error_property" in result)  # Output: False (exception was caught and ignored)

# Database save excludes properties by default
from rococo.repositories import BaseRepository
repository = BaseRepository(adapter, Product, message_adapter, "product_queue")
repository.save(product)  # Saves: name, price, tax_rate, category (no computed properties)
```

##### Property Inheritance

Properties are inherited from base classes and included in serialization:

```python
from dataclasses import dataclass
from rococo.models import VersionedModel

@dataclass
class BaseEntity(VersionedModel):
    created_by: str = "system"
    
    @property
    def audit_info(self) -> dict:
        """Base audit information."""
        return {
            "created_by": self.created_by,
            "entity_type": self.__class__.__name__
        }

@dataclass
class Document(BaseEntity):
    title: str = "Untitled"
    content: str = ""
    
    @property
    def word_count(self) -> int:
        """Count words in document content."""
        return len(self.content.split()) if self.content else 0
    
    @property
    def summary(self) -> dict:
        """Document summary including inherited properties."""
        base_info = self.audit_info  # Inherited property
        return {
            **base_info,
            "title": self.title,
            "word_count": self.word_count,
            "has_content": bool(self.content)
        }

doc = Document(title="My Document", content="Hello world example", created_by="user123")

result = doc.as_dict()
print(result["audit_info"])  # Output: {"created_by": "user123", "entity_type": "Document"}
print(result["word_count"])  # Output: 3
print(result["summary"])     # Output: Combined information from base and derived properties
```

##### API Response Usage

Calculated properties are particularly useful for API responses where you need computed fields:

```python
from dataclasses import dataclass
from typing import List
from rococo.models import VersionedModel

@dataclass
class Order(VersionedModel):
    customer_name: str = ""
    items: List[dict] = None
    discount_percent: float = 0.0
    
    def __post_init__(self):
        if self.items is None:
            self.items = []
    
    @property
    def subtotal(self) -> float:
        """Calculate subtotal before discount."""
        return sum(item.get("price", 0) * item.get("quantity", 0) for item in self.items)
    
    @property
    def discount_amount(self) -> float:
        """Calculate discount amount."""
        return self.subtotal * (self.discount_percent / 100)
    
    @property
    def total(self) -> float:
        """Calculate final total."""
        return self.subtotal - self.discount_amount
    
    @property
    def api_response(self) -> dict:
        """Complete API response format."""
        return {
            "order_id": str(self.entity_id),
            "customer": self.customer_name,
            "item_count": len(self.items),
            "pricing": {
                "subtotal": self.subtotal,
                "discount": self.discount_amount,
                "total": self.total
            },
            "status": "calculated"
        }

order = Order(
    customer_name="John Doe",
    items=[
        {"name": "Widget", "price": 10.0, "quantity": 2},
        {"name": "Gadget", "price": 25.0, "quantity": 1}
    ],
    discount_percent=10.0
)

# For API responses, include all calculated properties
api_data = order.as_dict(export_properties=True)
print(api_data["subtotal"])      # Output: 45.0
print(api_data["total"])         # Output: 40.5
print(api_data["api_response"])  # Output: Complete formatted response

# For database storage, exclude calculated properties
from rococo.repositories import BaseRepository
repository = BaseRepository(adapter, Order, message_adapter, "order_queue")
repository.save(order)  # Saves: customer_name, items, discount_percent (no calculated fields)
```

**Key Features:**
- **Automatic Inclusion**: Properties are included in `as_dict()` by default (`export_properties=True`)
- **Database Optimization**: Repositories exclude properties from database saves by default (`save_calculated_fields=False`)
- **Configurable Behavior**: Control property inclusion with `export_properties` parameter and repository settings
- **Exception Handling**: Properties that raise exceptions are gracefully excluded from serialization
- **Inheritance Support**: Properties from base classes are automatically included
- **API Friendly**: Perfect for computed fields in API responses without database storage overhead
- **Performance Aware**: Properties are only computed when accessed, not stored redundantly

#### Messaging

##### RabbitMQ

```python
# Producer
from rococo.messaging import RabbitMqConnection

with RabbitMqConnection('host', 'port', 'username', 'password', 'virtual_host') as conn:
    conn.send_message('queue_name', {'message': 'data'})


# Consumer
from rococo.messaging import RabbitMqConnection

def process_message(message_data: dict):
    print(f"Processing message {message_data}...")

with RabbitMqConnection('host', 'port', 'username', 'password', 'virtual_host') as conn:
    conn.consume_messages('queue_name', process_message)
```

##### SQS

```python
# Producer
from rococo.messaging import SqsConnection

with SqsConnection(region_name='us-east-1') as conn:
    conn.send_message('queue_name', {'message': 'data'})


# Consumer
from rococo.messaging import SqsConnection

def process_message(message_data: dict):
    print(f"Processing message {message_data}...")

with SqsConnection(region_name='us-east-1') as conn:
    conn.consume_messages('queue_name', process_message)

# Note: since cleanup is not required for SQS connections, you can also do:
conn = SqsConnection(region_name='us-east-1')
conn.send_message('queue_name', {'message': 'data'})
conn.consume_messages('queue_name', process_message)
```

##### Processing

Processing data from messages can be achieved by implementing the abstract class `BaseServiceProcessor` within `messaging/base.py`

#### Data

##### SurrealDB

```python
from rococo.data import SurrealDbAdapter

def get_db_connection():
    endpoint = "ws://localhost:8000/rpc"
    username = "myuser"
    password = "mypassword"
    namespace = "test"
    database = "test"

    return SurrealDbAdapter(endpoint, username, password, namespace, database)


with get_db_connection() as db:
    db.execute_query("""insert into person {
        user: 'me',
        pass: 'very_safe',
        tags: ['python', 'documentation']
    };""")
    print(db.execute_query("SELECT * FROM person;", {}))
```
##### PostgreSQL

```python
from rococo.data import PostgreSQLAdapter

def get_db_connection():
    host = "http://localhost"
    port = "5432"
    username = "myuser"
    password = "mypassword"
    database = "test"

    return PostgreSQLAdapter(host, port, username, password, database)


with get_db_connection() as db:
    db.execute_query("""
    INSERT INTO cars (brand, model, year)
    VALUES ('Volvo', 'p1800', 1968)""")
    print(db.execute_query("SELECT * FROM cars;", {}))
```

<summary>

##### Relationships in Surreal DB

</summary>

<details>

Consider the following example models:

```python
# Models
from dataclasses import field, dataclass
from rococo.repositories import SurrealDbRepository
from rococo.models import VersionedModel
from rococo.data import SurrealDbAdapter

@dataclass
class Email(VersionedModel):
    email_address: str = None

@dataclass
class LoginMethod(VersionedModel):
    email: str = field(default=None, metadata={
        'relationship': {'model': Email, 'type': 'direct'},
        'field_type': 'record_id'
    })
    method_type: str = None

@dataclass
class Person(VersionedModel):
    login_method: str = field(default=None, metadata={
        'relationship': {'model': LoginMethod, 'type': 'direct'},
        'field_type': 'record_id'
    })
    name: str = None
    

@dataclass
class Organization(VersionedModel):
    person: str = field(default=None, metadata={
        'relationship': {'model': Person, 'type': 'direct'},
        'field_type': 'record_id'
    })
    name: str = None


def get_db_connection():
    endpoint = "ws://localhost:8000/rpc"
    username = "root"
    password = "root"
    namespace = "breton1"
    database = "bretondb1"
    return SurrealDbAdapter(endpoint, username, password, namespace, database)





# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email=email  # Can be referenced by object
)

# Create a Person that references LoginMethod object
person = Person(
    name="Axel",
    login_method=login_method.entity_id  # Can be referenced by UUID object.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person=str(person.entity_id)  # Can be referenced by UUID string.
)


with get_db_connection() as adapter:
    # **Create repositories**
    person_repo = SurrealDbRepository(adapter, Person, None, None)
    organization_repo = SurrealDbRepository(adapter, Organization, None, None)
    login_method_repo = SurrealDbRepository(adapter, LoginMethod, None, None)
    email_repo = SurrealDbRepository(adapter, Email, None, None)

    # ** Save objects.
    organization_repo.save(organization)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:00.816083",
    #     "id": "organization:⟨5bb0a0dc-0043-45a3-9dac-d514b5ef7669⟩",
    #     "name": "Organization1",
    #     "person": "person:⟨7a3f4e8c-fd46-43db-b619-5b2129bbcc37⟩",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "b49010ad-bc64-487e-bd41-4cdf20ff7aab"
    # }

    person_repo.save(person)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:00.959270",
    #     "id": "person:⟨7a3f4e8c-fd46-43db-b619-5b2129bbcc37⟩",
    #     "login_method": "loginmethod:⟨0e1ef122-e4aa-435f-ad97-bd75ef6d1eb8⟩",
    #     "name": "Person1",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "95049030-80bd-45cd-a39f-09139fe67343"
    # }

    login_method_repo.save(login_method)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:01.025179",
    #     "email": "email:⟨3e654628-47fa-4a0e-bd42-79fda124149e⟩",
    #     "id": "loginmethod:⟨0e1ef122-e4aa-435f-ad97-bd75ef6d1eb8⟩",
    #     "method_type": "email-password",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "9e20a1dc-bcb1-45c2-bdf8-64e441a79758"
    # }

    email_repo.save(email)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:01.093089",
    #     "email_address": "test@example.com",
    #     "id": "email:⟨3e654628-47fa-4a0e-bd42-79fda124149e⟩",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "0f693d94-a912-4b0a-bc96-3e558f7e13d5"
    # }


    # **Fetching related objects
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, fetch_related=['person'])
    # Roughly evaluates to "SELECT * FROM organization FETCH person;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    print(organization.person.name)  # Prints name of related person

    organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # Roughly evaluates to "SELECT * FROM organization;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    try:
        print(organization.person.name)  # raises AttributeError
    except AttributeError:
        pass

    print(organization.as_dict(True))
    # prints 
    # {
    #     "entity_id":"ff02a2c0-6bcf-426f-b5b9-8c01913b79f6",
    #     "version":"9d58fd0e-b70a-4772-91f9-af3e6342de5b",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active": True,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2023-11-25T12:21:09.028676",
    #     "person":{
    #         "entity_id":"93cc132c-2a2a-46e4-853a-c02239336a28"
    #     },
    #     "name":"Organization1"
    # }

    # FETCH chaining
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, fetch_related=['person', 'person.login_method', 'person.login_method.email'])
    # Roughly evaluates to "SELECT * FROM organization FETCH person, person.login_method, person.login_method.email"

    print(organization.entity_id)  # Prints entity_id of organization
    print(organization.person.entity_id)  # Prints entity_id of organization.person
    print(organization.person.login_method.entity_id)  # Prints entity_id of organization.person.login_method
    print(organization.person.login_method.email.entity_id)  # Prints entity_id of organization.person.login_method.email
    print(organization.person.login_method.email.email_address)  # Prints email address of organization.person.login_method.email
    print(organization.as_dict(True))
    # prints
    # {
    #     "entity_id":"846f0756-20ab-44d3-8899-07e10b698ccd",
    #     "version":"578ca4c7-311a-4508-85ec-00ba264cd741",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active": True,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2023-11-25T12:19:46.541387",
    #     "person":{
    #         "entity_id":"ef99b93c-e1bb-4f37-96d5-e4e560dbdda0",
    #         "version":"b3ce7b8a-223e-4a63-a842-042178c9645c",
    #         "previous_version":"00000000-0000-4000-8000-000000000000",
    #         "active": True,
    #         "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #         "changed_on":"2023-11-25T12:19:46.623192",
    #         "login_method":{
    #             "entity_id":"a7efa334-ea92-4e59-95d5-c8d51a976c1b",
    #             "version":"4d1a9a1b-81fb-433f-8b43-1bf1c3b696da",
    #             "previous_version":"00000000-0000-4000-8000-000000000000",
    #             "active": True,
    #             "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #             "changed_on":"2023-11-25T12:19:46.706244",
    #             "email":{
    #                 "entity_id":"76e95956-0404-4c06-916b-89927b73d26d",
    #                 "version":"d59a91a4-26ef-4a88-bee2-b5c0d651bd77",
    #                 "previous_version":"00000000-0000-4000-8000-000000000000",
    #                 "active":True,
    #                 "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #                 "changed_on":"2023-11-25T12:19:46.775098",
    #                 "email_address":"test@example.com"
    #             },
    #             "method_type":"email-password"
    #         },
    #         "name":"Person1"
    #     },
    #     "name":"Organization1"
    # }
```

###### Many-to-many relationships

```python
# Many-to-Many relationships

# **Creating and relating objects.**
# Many-to-Many relationships
# Investor->investswith->Investment
#         /\     /\    /\
#         ||     ||    ||
#         IN    name   OUT

from typing import List

@dataclass
class Investor(VersionedModel):
    name: str = None
    # One-to-Many field
    person: str = field(default=None, metadata={
        'relationship': {'model': Person, 'type': 'direct'},
        'field_type': 'record_id'
    })
    # Many-to-Many field
    investments: List[VersionedModel] = field(default=None, metadata={
        'relationship': {'model': 'Investment', 'type': 'associative', 'name': 'investswith', 'direction': 'out'},
        'field_type': 'm2m_list'
    })

@dataclass
class Investment(VersionedModel):
    name: str = None
    # Many-to-Many field
    investors: List[VersionedModel] = field(default=None, metadata={
        'relationship': {'model': 'Investor', 'type': 'associative', 'name': 'investswith', 'direction': 'in'},
        'field_type': 'm2m_list'
    })


# **Creating and relating objects.**
investor1 = Investor(name="Investor1", person=person)
investor2 = Investor(name="Investor2", person=person.entity_id)
investor3 = Investor(name="Investor3", person=Person(entity_id=person.entity_id))
investment1 = Investment(name="Investment1")
investment2 = Investment(name="Investment2")
investment3 = Investment(name="Investment3")

with get_db_connection() as adapter:
    # **Create repositories**
    investor_repo = SurrealDbRepository(adapter, Investor, None, None)
    investment_repo = SurrealDbRepository(adapter, Investment, None, None)

    investor_repo.save(investor1)
    investor_repo.save(investor2)
    investor_repo.save(investor3)
    investment_repo.save(investment1)
    investment_repo.save(investment2)
    investment_repo.save(investment3)

    # Relate investor1 to investment2 and investment3
    investor_repo.relate(investor1, 'investswith', investment2)
    # OR
    investment_repo.relate(investor1, 'investswith', Investment(entity_id=investment3.entity_id))

    # Relate investor2 to investment1 and investment3
    investor_repo.relate(Investor(entity_id=investor2.entity_id), 'investswith', investment1)
    investor_repo.relate(investor2, 'investswith', investment3)

    # Relate investor3 to investment1 and investment2
    investment_repo.relate(investor3, 'investswith', investment1)
    investment_repo.relate(investor3, 'investswith', investment2)


    # Fetching many-to-many relations
    for investment in investment_repo.get_many({}, fetch_related=['investors']):
        print("Investment: ", investment.as_dict(True))
        print()

    # Fetch-chaining for many-to-many relations
    for investment in investment_repo.get_many({}, fetch_related=['investors', 'investors.person', 'investors.person.login_method', 'investors.person.login_method.email']):
        print("Investment: ", investment.as_dict(True))
        print()

    # Get investments of an investor by investor's entity_id
    investor_with_investments = investor_repo.get_one({'entity_id': investor1.entity_id}, fetch_related=['investments'])
    investments = investor_with_investments.investments
    for investment in investments:
        print(investment.as_dict())

```

</details>

<summary>

##### Relationships in MySQL

</summary>

<details>

Consider the following example models:

```python
# Models
from dataclasses import field, dataclass
from rococo.repositories.mysql import MySqlRepository
from rococo.models import VersionedModel
from rococo.data import MySqlAdapter

@dataclass
class Email(VersionedModel):
    email_address: str = None

@dataclass
class LoginMethod(VersionedModel):
    email_id: str = None  # Stores the entity_id of an object of Email class.
    method_type: str = None

@dataclass
class Person(VersionedModel):
    login_method_id: str = None  # Stores the entity_id of an object of LoginMethod class.
    name: str = None
    

@dataclass
class Organization(VersionedModel):
    person_id: str = None  # Stores the entity_id of an object of Person class.
    name: str = None


def get_db_connection():
    return MySqlAdapter('localhost', 3306, 'root', 'ransomsnare_root_pass', 'testdb')


# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email_id=email.entity_id  # Reference to the Email object created previously.
)

# Create a Person that references LoginMethod object
person = Person(
    name="Axel",
    login_method_id=login_method.entity_id  # Reference to the LoginMethod object created previously.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person_id=person.entity_id  # Reference to the Person object created previously.
)


with get_db_connection() as adapter:
    # **Create repositories**
    person_repo = MySqlRepository(adapter, Person, None, None)
    organization_repo = MySqlRepository(adapter, Organization, None, None)
    login_method_repo = MySqlRepository(adapter, LoginMethod, None, None)
    email_repo = MySqlRepository(adapter, Email, None, None)

    # ** Save objects.
    organization_repo.save(organization)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "entity_id": "5bb0a0dc004345a39dacd514b5ef7669",
    #     "name": "Organization1",
    #     "person_id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "b49010adbc64487ebd414cdf20ff7aab"
    # }

    person_repo.save(person)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "login_method_id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "name": "Axel",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9504903080bd45cda39f09139fe67343"
    # }

    login_method_repo.save(login_method)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email_id": "3e65462847fa4a0ebd4279fda124149e",
    #     "id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "method_type": "email-password",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9e20a1dcbcb145c2bdf864e441a79758"
    # }

    email_repo.save(email)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email_address": "test@example.com",
    #     "id": "3e65462847fa4a0ebd4279fda124149e",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "0f693d94a9124b0abc963e558f7e13d5"
    # }


    # **Fetching related objects
    organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # Roughly evaluates to "SELECT * FROM organization WHERE entity_id=<Specified entity ID> LIMIT 1;"

    print(organization.person_id)  # Prints entity_id of related person
    print(organization.as_dict(True))

    # prints 
    # {
    #     "entity_id":"fb5a9d0e-4bac-467f-9318-4063811e51b6",
    #     "version":"6fb045ef-1428-4a0c-b5a6-37c18e6711ab",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:03:21",
    #     "person_id": "582ecaade30f40bc8e6cc4675a4bc178",
    #     "name":"Organization1"
    # }
    
    person = person_repo.get_one({"entity_id": organization.person_id})

    # Get all organizations by person
    person_orgs = organization_repo.get_many({
        "person_id": person.entity_id
    })
    for org in person_orgs:
        print(org.as_dict(True))
    # Prints:
    # {
    #     "entity_id":"0af9964d-0fc7-4128-ba7f-a66a51a87231",
    #     "version":"ce694166-5ca6-43dc-936d-078011469465",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:14:07",
    #     "person_id": "5b10a75a-23d7-4b98-b35e-0f1a59ec5b6d",
    #     "name":"Organization1"
    # }

```

</details>

<summary>

##### Relationships in PostgreSQL

</summary>

<details>

Consider the following example models:

```python
# Models
from dataclasses import field, dataclass
from rococo.repositories.postgresql import PostgreSQLRepository
from rococo.models import VersionedModel
from rococo.data import PostgreSQLAdapter

@dataclass
class Email(VersionedModel):
    email_address: str = None

@dataclass
class LoginMethod(VersionedModel):
    email_id: str = None  # Stores the entity_id of an object of Email class.
    method_type: str = None

@dataclass
class Person(VersionedModel):
    login_method_id: str = None  # Stores the entity_id of an object of LoginMethod class.
    name: str = None
    

@dataclass
class Organization(VersionedModel):
    person_id: str = None  # Stores the entity_id of an object of Person class.
    name: str = None


def get_db_connection():
    return PostgreSQLAdapter('localhost', 5432, 'postgres', 'ransomsnare_root_pass', 'testdb')


# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email_id=email.entity_id  # Reference to the Email object created previously.
)

# Create a Person that references LoginMethod object
person = Person(
    name="Axel",
    login_method_id=login_method.entity_id  # Reference to the LoginMethod object created previously.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person_id=person.entity_id  # Reference to the Person object created previously.
)


with get_db_connection() as adapter:
    # **Create repositories**
    person_repo = PostgreSQLRepository(adapter, Person, None, None)
    organization_repo = PostgreSQLRepository(adapter, Organization, None, None)
    login_method_repo = PostgreSQLRepository(adapter, LoginMethod, None, None)
    email_repo = PostgreSQLRepository(adapter, Email, None, None)

    # ** Save objects.
    organization_repo.save(organization)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "entity_id": "5bb0a0dc004345a39dacd514b5ef7669",
    #     "name": "Organization1",
    #     "person_id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "b49010adbc64487ebd414cdf20ff7aab"
    # }

    person_repo.save(person)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "login_method_id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "name": "Axel",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9504903080bd45cda39f09139fe67343"
    # }

    login_method_repo.save(login_method)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email_id": "3e65462847fa4a0ebd4279fda124149e",
    #     "id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "method_type": "email-password",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9e20a1dcbcb145c2bdf864e441a79758"
    # }

    email_repo.save(email)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email_address": "test@example.com",
    #     "id": "3e65462847fa4a0ebd4279fda124149e",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "0f693d94a9124b0abc963e558f7e13d5"
    # }


    # **Fetching related objects
    organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # Roughly evaluates to "SELECT * FROM organization WHERE entity_id=<Specified entity ID> LIMIT 1;"

    print(organization.person_id)  # Prints entity_id of related person
    print(organization.as_dict(True))

    # prints 
    # {
    #     "entity_id":"fb5a9d0e-4bac-467f-9318-4063811e51b6",
    #     "version":"6fb045ef-1428-4a0c-b5a6-37c18e6711ab",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:03:21",
    #     "person_id": "582ecaade30f40bc8e6cc4675a4bc178",
    #     "name":"Organization1"
    # }
    
    person = person_repo.get_one({"entity_id": organization.person_id})

    # Get all organizations by person
    person_orgs = organization_repo.get_many({
        "person_id": person.entity_id
    })
    for org in person_orgs:
        print(org.as_dict(True))
    # Prints:
    # {
    #     "entity_id":"0af9964d-0fc7-4128-ba7f-a66a51a87231",
    #     "version":"ce694166-5ca6-43dc-936d-078011469465",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:14:07",
    #     "person_id": "5b10a75a-23d7-4b98-b35e-0f1a59ec5b6d",
    #     "name":"Organization1"
    # }

```

</details>

### How to use the adapter and base Repository in another projects

```python
class LoginMethodRepository(BaseRepository):
    def __init__(self, adapter, message_adapter, queue_name):
        super().__init__(adapter, LoginMethod, message_adapter, queue_name)

    def save(self, login_method: LoginMethod, send_message: bool = False):
        with self.adapter:
            return super().save(login_method,send_message)

    def get_one(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_one(conditions)

    def get_many(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_many(conditions)

```

- The LoginMethodRepository class is a concrete implementation of the BaseRepository class. It is responsible for managing LoginMethod objects in the database.

    The __init__() method takes an adapter object as input. This adapter object is responsible for communicating with the database. The adapter object is passed to the super().__init__() method, which initializes the base repository class.
    It also takes in a message adapter and queue name for RabbitMQ and SQS messaging which can later be used in the save() method by passing a boolean.

    The save() method takes a LoginMethod object as input and saves it to the database. The get_one() method takes a dictionary of conditions as input and returns a single LoginMethod object that matches those conditions. The get_many() method takes a dictionary of conditions as input and returns a list of LoginMethod objects that match those conditions.

### Repository Configuration Options

All repositories inherit from `BaseRepository` and support several configuration options that control their behavior:

#### Auditing Control

By default, repositories use audit tables to maintain version history. This can be controlled using the `use_audit_table` property:

```python
from rococo.repositories import BaseRepository
from rococo.data import PostgreSQLAdapter

# Default behavior: auditing enabled
repository = BaseRepository(
    adapter=db_adapter,
    model=MyModel,
    message_adapter=message_adapter,
    queue_name="my_queue"
)

print(repository.use_audit_table)  # Output: True

# Disable auditing for this repository
repository.use_audit_table = False

# When saving, previous versions won't be moved to audit tables
repository.save(my_model_instance)
```

#### TTL (Time To Live) Fields for MongoDB

**MongoDB repositories only** support TTL (Time To Live) functionality for automatic document expiration. This is particularly useful for implementing data retention policies on deleted records:

```python
from rococo.repositories.mongodb import MongoDbRepository
from rococo.data import MongoDBAdapter

# Create MongoDB repository with TTL configuration
repository = MongoDbRepository(
    db_adapter=mongodb_adapter,
    model=MyModel,
    message_adapter=message_adapter,
    queue_name="my_queue"
)

# Configure TTL for deleted records
repository.ttl_field = "expires_at"      # Field name for TTL timestamp
repository.ttl_minutes = 30              # TTL duration in minutes

# When deleting a record, TTL timestamp will be automatically added
my_model = MyModel(name="Test Record")
repository.create(my_model, "my_collection")

# Delete the record - TTL field will be added automatically
deleted_model = repository.delete(my_model, "my_collection")

# The deleted document will now have an "expires_at" field set to
# current_time + 30 minutes, and MongoDB will automatically remove
# the document after that time expires
```

**TTL Configuration Details:**

- **`ttl_field`**: The name of the field that will store the TTL timestamp (e.g., "expires_at", "delete_after")
- **`ttl_minutes`**: The number of minutes from deletion time when the document should expire
- **MongoDB Index**: You need to create a TTL index on the specified field in MongoDB:
  ```javascript
  // In MongoDB shell
  db.my_collection.createIndex({"expires_at": 1}, {expireAfterSeconds: 0})
  ```

**TTL Usage Examples:**

```python
# Different TTL configurations for different use cases
class UserRepository(MongoDbRepository):
    def __init__(self, db_adapter, message_adapter, queue_name):
        super().__init__(db_adapter, User, message_adapter, queue_name)
        # Deleted users expire after 90 days
        self.ttl_field = "scheduled_deletion"
        self.ttl_minutes = 90 * 24 * 60  # 90 days in minutes

class SessionRepository(MongoDbRepository):
    def __init__(self, db_adapter, message_adapter, queue_name):
        super().__init__(db_adapter, Session, message_adapter, queue_name)
        # Deleted sessions expire after 1 hour
        self.ttl_field = "expires_at"
        self.ttl_minutes = 60  # 1 hour

class LogRepository(MongoDbRepository):
    def __init__(self, db_adapter, message_adapter, queue_name):
        super().__init__(db_adapter, LogEntry, message_adapter, queue_name)
        # Deleted logs expire after 7 days
        self.ttl_field = "cleanup_after"
        self.ttl_minutes = 7 * 24 * 60  # 7 days in minutes

# Usage
user_repo = UserRepository(mongodb_adapter, message_adapter, "user_queue")
session_repo = SessionRepository(mongodb_adapter, message_adapter, "session_queue")
log_repo = LogRepository(mongodb_adapter, message_adapter, "log_queue")

# When deleting, each repository will apply its own TTL settings
user_repo.delete(user_instance, "users")        # Expires in 90 days
session_repo.delete(session_instance, "sessions")  # Expires in 1 hour
log_repo.delete(log_instance, "logs")           # Expires in 7 days
```

**Important Notes:**
- TTL functionality is **only available for MongoDB repositories** (`MongoDbRepository`)
- TTL fields are only added during `delete()` operations, not during regular saves
- If `ttl_field` is `None` (default), no TTL timestamp is added
- The actual document expiration is handled by MongoDB's TTL feature, not by the repository
- You must create appropriate TTL indexes in MongoDB for the expiration to work

#### Calculated Fields Control

Control whether computed properties are included in database saves:

```python
# Default behavior: calculated fields excluded from database saves
repository = BaseRepository(
    adapter=db_adapter,
    model=MyModel,
    message_adapter=message_adapter,
    queue_name="my_queue"
)

print(repository.save_calculated_fields)  # Output: False

# Include calculated fields in database saves
repository.save_calculated_fields = True

# Now @property methods will be included when saving to database
repository.save(my_model_instance)
```

**Key Repository Configuration Features:**
- **Auditing Control**: Enable/disable audit table usage with `use_audit_table`
- **TTL Support**: MongoDB-only feature for automatic document expiration using `ttl_field` and `ttl_minutes`
- **Calculated Fields**: Control computed property inclusion in database saves with `save_calculated_fields`
- **Database Agnostic**: Most features work across all supported databases (PostgreSQL, MySQL, SurrealDB, MongoDB)
- **Flexible Configuration**: Settings can be configured per repository instance for different data retention policies

#### RepositoryFactory

```python
class RepositoryFactory:
    _repositories = {}

    @classmethod
    def _get_db_connection(cls):
        endpoint = "ws://localhost:8000/rpc"
        username = "myuser"
        password = "mypassword"
        namespace = "hell"
        db_name = "abclolo"
        return SurrealDbAdapter(endpoint, username, password, namespace, db_name)

    @classmethod
    def get_repository(cls, repo_class: Type[BaseRepository]):
        if repo_class not in cls._repositories:
            adapter = cls._get_db_connection()
            cls._repositories[repo_class] = repo_class(adapter)
        return cls._repositories[repo_class]

```

- The RepositoryFactory class is a singleton class that is responsible for creating and managing repositories. It uses a cache to store the repositories that it has already created. This allows it to avoid creating the same repository multiple times.

    The _get_db_connection() method creates a new database connection using the specified endpoint, username, password, namespace, and database name. The get_repository() method takes a repository class as input and returns the corresponding repository object. If the repository object does not already exist in the cache, then the factory will create a new one and add it to the cache.

#### Sample usage

```python
sample_data = LoginMethod(
    person_id="asd123123",
    method_type="email",
    method_data={},
    email="user@example.com",
    password="hashed_password",
)

repo = RepositoryFactory.get_repository(LoginMethodRepository)

result = repo.save(sample_data)

print("Done", repo.get_one({}))
```

- The above code creates a new LoginMethod object and saves it to the database using the LoginMethodRepository object. It then retrieves the saved object from the database and prints it to the console.

    This is just a simple example of how to use the LoginMethodRepository and RepositoryFactory classes. You can use these classes to manage any type of object in a database.


### Rococo MySQL CLI (`rococo-mysql`)

This CLI interface provides commands for managing MySQL migrations using the Rococo module. It supports creating new migrations, running forward and backward migrations, and retrieving the current database version. The CLI also handles environment variables from `.env` files for database connection configurations.

#### Usage

```bash
rococo-mysql [OPTIONS] COMMAND
```

#### Options

- `--migrations-dir` (optional): Path to the migrations directory of your project. Defaults to checking standard directories (`flask/app/migrations`, `api/app/migrations`, `app/migrations`).
- `--env-files` (optional): Paths to environment files containing database connection details (e.g., `.env.secrets`, `<APP_ENV>.env`).

#### Commands

##### `new`
Creates a new migration file in the specified migrations directory.

```bash
rococo-mysql new
```

##### `rf`
Runs the forward migration, applying all unapplied migrations in sequence.

```bash
rococo-mysql rf
```

##### `rb`
Runs the backward migration, rolling back the last applied migration.

```bash
rococo-mysql rb
```

##### `version`
Displays the current database version.

```bash
rococo-mysql version
```

#### Environment Configuration

- If no `--env-files` are provided, the CLI attempts to load environment variables from `.env.secrets` and an environment-specific `<APP_ENV>.env` file.
- The environment variables required for the database connection are:
  - `MYSQL_HOST`
  - `MYSQL_PORT`
  - `MYSQL_USER`
  - `MYSQL_PASSWORD`
  - `MYSQL_DATABASE`

#### Example

Running a forward migration:

```bash
rococo-mysql --migrations-dir=app/migrations --env-files=.env .env.secrets rf
```

This command runs all pending migrations using the specified environment files and migrations directory.


### Using [`email-transmitter`](https://github.com/EcorRouge/email-transmitter) in your project

- Create an `email_transmitter` directory in your project under `services` directory.
- Add a `config.json` file in the `email_transmitter` directory. No other file is needed in this directory.
- The `config.json` should contain a configuration object with the following keys:
    - `configurations`: A list of configuration objects. Currently, we are only using `mailjet` provider. An example config for `mailjet` provider looks like:
        ```json
        [
            {
                "provider": "mailjet",
                "sourceEmail": "EcorRouge <system@ecorrouge.com>",
                "errorReportingEmail": "system@ecorrouge.com"
            }
        ]
        ```
    - `events`: An object whose keys represent an event name and the value represents an object that represents the email to be sent when that event is received. An example `events` object looks like:
        ```json
        {
            "USER_CREATED": {
                "subject": "Welcome {{var:recipient_name}}",
                "templateName": "Welcome (PROD and TEST)",
                "id": {
                    "mailjet": 4777555
                }
            }
        }
        ```
- **Example `config.json`**:
    ```json
    {
        "configurations": [
            {
                "provider": "mailjet",
                "sourceEmail": "EcorRouge <system@ecorrouge.com>",
                "errorReportingEmail": "system@ecorrouge.com"
            }
        ],
        "events": {
            "USER_CREATED": {
                "subject": "Welcome {{var:recipient_name}}",
                "templateName": "Welcome (PROD and TEST)",
                "id": {
                    "mailjet": 4777555
                }
            }
        }
    }
    ```

- Add the `email_transmitter` service to `docker-compose.yml`. A simple definition looks like:
```yaml
services:
  email_transmitter:
    image: ecorrouge/email-transmitter:latest
    container_name: project_email_transmitter
    restart: unless-stopped
    env_file:
      - ../.env  # Path to .env
    volumes:
      - <path_to_email_transmitter_service>/config.json:/app/src/services/email_transmitter/src/config.json
```

  - Make sure `MAILJET_API_KEY` and `MAILJET_API_SECRET` are available in the provided `env_file` file(s).
  - Make sure `EmailServiceProcessor_QUEUE_NAME` and `QUEUE_NAME_PREFIX` are available in the provided `env_file` file(s).
  - Make sure the following variables are also available in the provided `env_file` file(s):
    - `RABBITMQ_HOST`
    - `RABBITMQ_PORT`
    - `RABBITMQ_USER`
    - `RABBITMQ_PASSWORD`
    - `RABBITMQ_VIRTUAL_HOST`
  - Make sure the service is added to the same network as rest of the services that are to going to be calling this service.

- **How to call email-transmitter to send an email from application code**:
```python
from rococo.messaging import RabbitMqConnection

EMAIL_TRANSMITTER_QUEUE_NAME = os.getenv('QUEUE_NAME_PREFIX') + os.getenv('EmailServiceProcessor_QUEUE_NAME')

user_created = User(...)

message = {
    "event": "USER_CREATED",  # The event to trigger as defined in `email_transmitter/config.json`.
    "data": {  # The data to be passed to the email template specfied against the event in config.json.
        "confirmation_link": confirmation_link,
        "recipient_name": user.name,
    },
    "to_emails": [user.email],  # A list of email addresses where the email should be sent.
}


with RabbitMqConnection('host', 'port', 'username', 'password', 'virtual_host') as conn:
    conn.send_message(EMAIL_TRANSMITTER_QUEUE_NAME, message)
```



### Deployment

The process described is a Continuous Integration (CI) and Continuous Deployment (CD) pipeline for a Python package using _GitHub Actions_. Here's the breakdown:

### Development Phase

Developers push their changes directly to the main branch.
This branch is likely used for ongoing development work.

### Staging/Testing Phase

When the team is ready to test a potential release, they push the code to a staging branch.
Once the code is pushed to this branch, _GitHub Actions_ automatically publishes the package to the test PyPi server.
The package can then be reviewed and tested by visiting <https://test.pypi.org/project/rococo/>.
This step ensures that the package works as expected on the PyPi platform without affecting the live package.

### Release/Publish Phase

When the team is satisfied with the testing and wants to release the package to the public, they create and publish a release on the GitHub repository.
Following this action, _GitHub Actions_ takes over and automatically publishes the package to the official PyPi server.
The package can then be accessed and downloaded by the public at <https://pypi.org/project/rococo/>.

In essence, there are three primary phases:

1. Development (main branch)
2. Testing (staging branch with test PyPi server)
3. Release (triggered by a GitHub release and published to the official PyPi server).


### Local Development

To install local Rococo version in other project, upload to your PyPi:
1) Run command "python setup.py sdist" to generate tar.gz file that will be uploaded to PyPi
2) create ./pypirc file in the root of the directory and add:
[pypi]
    username = __token__
    password = THE_TOKEN_PROVIDED_BY_PYPI
3) run the command: twine upload --config-file=./.pypirc dist/*
