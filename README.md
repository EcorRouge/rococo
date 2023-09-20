# rococo
A Python library to help build things the way we want them built.


## Basic Usage

### Installation

Install using pip:

```bash
pip install rococo
```

### Example

```python
from rococo.models import Person

# Initialize a Person object from rococo's built-in models.
someone = Person(first_name="John", last_name="Doe")

# Prepare to save the object in the database adding/updating attributes for the object.
someone.prepare_for_save(changed_by_id="jane_doe")

someone.as_dict()

{
    'active': True,
    'changed_by_id': 'jane_doe',
    'changed_on': datetime.datetime(2023, 9, 20, 19, 50, 23, 532875),
    'entity_id': 'e06876705b364640a20efc165f6ffb76',
    'first_name': 'John',
    'last_name': 'Doe',
    'previous_version': '7e63a5d0aa0f43b5aa9c8cc0634c41f2',
    'version': '08489d2bc5d74f78b7af0f2c1d9c5498'
}
```

