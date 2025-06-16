# Rococo Models Spec

This spec outlines how to define and work with data models using the `rococo` library. Rococo builds on top of dataclasses to provide model definitions.

All models are defined as dataclasses, and the library provides a set of tools to work with these models, including validation, serialization, and deserialization.

All models are defined in `common/models/<model_name>.py` files and imported in `common/models/__init__.py` files.


## Model Definition

All models must inherit from `rococo.models.VersionedModel` class. This class defines default fields and methods which are common to all models.

The default fields common to all models are:
- `entity_id` (str): A unique identifier for the entity. It is a UUID (without dashes) and is generated automatically.
- `version` (str): A version identifier for the entity. It is a UUID (without dashes) and is generated automatically.
- `previous_version` (str): The identifier of the previous version of the entity. It is a UUID (without dashes) and is generated automatically.
- `active` (bool): A flag indicating whether the entity is active or not. It defaults to `True`.
- `changed_by_id` (str): The identifier of the user who last changed the entity. It is a UUID (without dashes) and is generated automatically.
- `changed_on` (datetime): The date and time when the entity was last changed. It is generated automatically.

Common methods for all models include:
- `def as_dict(self, convert_datetime_to_iso_string: bool = False, convert_uuids: bool = True) -> Dict[str, Any]:`
    Converts the model instance to a dictionary representation. It can convert datetime fields to ISO strings and UUIDs to strings.

- `def from_dict(cls, data: Dict[str, Any]) -> VersionedModel:`
    Creates a model instance from a dictionary representation. It can handle nested models and lists of models.

- `def validate(self) -> None:`
    Validates the model instance. It checks for required fields and validates the data types. It raises a `ValidationError` if validation fails.

- `use_type_checking: ClassVar[bool] = False`
    This is a class variable that indicates whether type checking should be used. It is set to `False` by default.


## Model Example

```python
from datetime import datetime
from typing import Dict, Any
from rococo.models import VersionedModel
from enum import StrEnum

class OrganizationTypeEnum(StrEnum):
    COMPANY = "company"
    BROKER = "broker"
    CARRIER = "carrier"

    def __repr__(self):
        return str(self.value)

    @classmethod
    def values(cls):
        return [v.value for v in cls.__members__.values() if isinstance(v, cls)]


@dataclass
class Organization(BaseVersionedModel):
    use_type_checking: ClassVar[bool] = True

    name: str = ""
    organization_type: OrganizationTypeEnum = ""
    organization_website: Optional[str] = None
    number_of_employees: Optional[int] = None
    revenue: Optional[str] = None
    company_address: Optional[str] = None
    person_id: Optional[str] = None


    def validate_number_of_employees(self):
        field_value = self.number_of_employees
        if type(field_value) is str:
            field_value = re.sub(r'\D', '', field_value)

        try:
            self.number_of_employees = int(field_value)
        except (TypeError, ValueError):
            self.number_of_employees = None

    def validate_name(self):
        if self.name and type(self.name) is str:
            self.name = self.name[:128]

```


### Notes:

- Enums are used where needed to define a set of constant values. The `StrEnum` class is used to define string enums.
- The `validate_` methods will be called automatically before saving the model instance. They should return an error message if validation fails, or `None` if validation passes. Validations can also be silent by cleaning the field value.
- **No relationships**: The models should not define any relationships. For example, `person_id` is a string and not a reference to another model.
