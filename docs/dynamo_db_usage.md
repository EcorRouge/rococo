# DynamoDB Usage Guide

This guide explains how to use the DynamoDB adapter in Rococo. The adapter uses [PynamoDB](https://pynamodb.readthedocs.io/) under the hood , allowing you to work purely with Rococo `VersionedModel`s.

## Key Features

*   **Zero Config**: No need to define PynamoDB models manually.
*   **Dynamic Generation**: Automatically converts your Rococo models to PynamoDB models at runtime.
*   **Audit Support**: Automatically handles audit tables for versioning.
*   **Atomic Writes**: For `VersionedModel`s, updates are written using a DynamoDB transaction so the audit write and the new version write succeed or fail together.

## Installation

To use the DynamoDB adapter, install Rococo with the `data-dynamodb` extra:

```bash
pip install rococo[data-dynamodb]
```

## Usage

### 1. Define Your Model

Define your business model by inheriting from `VersionedModel`. You don't need to do anything special for DynamoDB.

```python
from dataclasses import dataclass
from rococo.models import VersionedModel

@dataclass
class Person(VersionedModel):
    first_name: str = None
    last_name: str = None
    age: int = None
    metadata: dict = None
```

The adapter automatically maps Python types to DynamoDB attributes:
- `str` -> `UnicodeAttribute`
- `bool` -> `BooleanAttribute`
- `int`, `float` -> `NumberAttribute`
- `dict` -> `JSONAttribute`
- `list` -> `ListAttribute`

### 2. Initialize Adapter and Repository

The `DynamoDbAdapter` is stateless and requires no arguments. It reads credentials from standard AWS environment variables.

```python
import os
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.repositories.dynamodb import DynamoDbRepository

# Ensure AWS credentials are set in your environment
# os.environ['AWS_ACCESS_KEY_ID'] = '...'
# os.environ['AWS_SECRET_ACCESS_KEY'] = '...'
# os.environ['AWS_REGION'] = 'us-east-1'

# 1. Initialize the adapter
adapter = DynamoDbAdapter()

# 2. Initialize the repository with your model
# The adapter will dynamically generate the PynamoDB model for 'Person'
person_repo = DynamoDbRepository(
    adapter=adapter,
    model=Person
)
```

### 3. Perform Operations

You can now use the repository to perform standard CRUD operations.

#### Saving
```python
person = Person(first_name="John", last_name="Doe", age=30)
saved_person = person_repo.save(person)
print(f"Saved: {saved_person.entity_id}")
```

#### Fetching
```python
# Get by ID (uses efficient Query)
fetched = person_repo.get_one({'entity_id': saved_person.entity_id})

# Get by other fields (uses Scan)
results = person_repo.get_many({'first_name': 'John'})
```

#### Deleting
```python
# Soft delete (sets active=False)
person_repo.delete(saved_person)
```

## How it Works

When you pass your `Person` class to the `DynamoDbRepository`, the `DynamoDbAdapter` inspects it and dynamically creates a PynamoDB model class (e.g., `PynamoPerson`) in memory.

It sets up the following schema:
*   **Table Name**: Derived from the class name (e.g., `person`).
*   **Hash Key**: `entity_id`
*   **Attributes**: Mapped from your dataclass fields.

### Audit Tables
If you update a record, the adapter automatically moves the old version to an audit table (e.g., `person_audit`).
*   **Audit Table Name**: `{table_name}_audit`
*   **Hash Key**: `entity_id`
*   **Range Key**: `version`

Ensure your DynamoDB tables are created with these key schemas if you are provisioning them manually.

### Atomic Save + Audit (ACID)
When saving a `VersionedModel`, Rococo groups the audit insert and the main table write into a single DynamoDB `TransactWriteItems` operation.

This means:
- Either both the audit record and the new version are written, or neither is.
- Updates use an optimistic-lock condition based on `previous_version`.
- Creates use a "must not already exist" condition on `entity_id`.

## Example Application

Here is a complete example of a simple Book Library Tracker using Rococo and DynamoDB.

```python
# Simple Book Library Tracker using Rococo DynamoDB

import os
from dotenv import load_dotenv, find_dotenv
from dataclasses import dataclass
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, BooleanAttribute, NumberAttribute
from rococo.models import VersionedModel
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.repositories.dynamodb import DynamoDbRepository
from rococo.messaging import MessageAdapter

load_dotenv(find_dotenv())

# --- PynamoDB Models (for table creation) ---

class BookModel(Model):
    class Meta:
        table_name = 'book'
        region = 'us-east-1'

    entity_id = UnicodeAttribute(hash_key=True)
    version = UnicodeAttribute(null=True)
    previous_version = UnicodeAttribute(null=True)
    active = BooleanAttribute(default=True)
    changed_by_id = UnicodeAttribute(null=True)
    changed_on = UnicodeAttribute(null=True)
    
    title = UnicodeAttribute(null=True)
    author = UnicodeAttribute(null=True)
    status = UnicodeAttribute(null=True)  # "to_read", "reading", "finished"
    rating = NumberAttribute(null=True)   # 1-5 stars

class BookAuditModel(Model):
    class Meta:
        table_name = 'book_audit'
        region = 'us-east-1'

    entity_id = UnicodeAttribute(hash_key=True)
    version = UnicodeAttribute(range_key=True)
    previous_version = UnicodeAttribute(null=True)
    active = BooleanAttribute(default=True)
    changed_by_id = UnicodeAttribute(null=True)
    changed_on = UnicodeAttribute(null=True)
    title = UnicodeAttribute(null=True)
    author = UnicodeAttribute(null=True)
    status = UnicodeAttribute(null=True)
    rating = NumberAttribute(null=True)

# --- Rococo Model ---

@dataclass
class Book(VersionedModel):
    title: str = None
    author: str = None
    status: str = "to_read"  # to_read, reading, finished
    rating: int = None

# --- Mock Message Adapter ---

class MockMessageAdapter(MessageAdapter):
    def __init__(self): pass
    def send_message(self, queue_name, message):
        print(f"  Event: {queue_name}")

# --- Main Application ---

def main():
    print("Book Library Tracker\n")
    
    # Setup tables
    if not BookModel.exists():
        BookModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    if not BookAuditModel.exists():
        BookAuditModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    adapter = DynamoDbAdapter()
    repo = DynamoDbRepository(
        db_adapter=adapter,
        model=Book,
        message_adapter=MockMessageAdapter(),
        queue_name='book_events'
    )

    # Add a new book
    print("1. Adding a book...")
    book = Book(title="The Pragmatic Programmer", author="David Thomas", status="to_read")
    saved_book = repo.save(book)
    print(f"   Added: '{saved_book.title}' by {saved_book.author}")

    # Start reading it
    print("\n2. Starting to read...")
    saved_book.status = "reading"
    updated_book = repo.save(saved_book)
    print(f"   Status: {updated_book.status}")

    # Finish and rate it
    print("\n3. Finished reading!")
    updated_book.status = "finished"
    updated_book.rating = 5
    final_book = repo.save(updated_book)
    print(f"   Status: {final_book.status}, Rating: {'*' * final_book.rating}")

    # Fetch it back
    print("\n4. Fetching from library...")
    fetched = repo.get_one({'entity_id': final_book.entity_id})
    print(f"   '{fetched.title}' - {fetched.status} - {'*' * fetched.rating}")

if __name__ == "__main__":
    main()
```
