import pkgutil
import os
import importlib
import logging
from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar, is_dataclass
from datetime import datetime, timezone
from dateutil.parser import isoparse
from typing import Any, Dict, List, Union, get_type_hints, get_origin, get_args
from enum import Enum

logger = logging.getLogger(__name__)

# Constants for VersionedModel field groups
BIG_6_FIELDS = {'entity_id', 'version', 'previous_version',
                'changed_on', 'changed_by_id', 'active'}
BIG_6_UUID_FIELDS = ['entity_id', 'version',
                     'previous_version', 'changed_by_id']


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.now(timezone.utc)


def get_uuid_hex(_int=None):
    """
    Returns UUID in hex format. If _int is passed, it creates UUID with int base.
    """
    return uuid4().hex if _int is None else UUID(int=_int, version=4).hex


def import_models_module(current_module, module_name):
    """
    Dynamically import a module named `module_name` from the same tree as `current_module`
    """
    # First, try direct import (handles rococo.models via fallback)
    try:
        return importlib.import_module(module_name)
    except ImportError:
        # Fallback: if attempting to import 'models', resolve to rococo.models
        if module_name == 'models':
            return importlib.import_module('rococo.models')
        # Otherwise, continue to filesystem-based lookup

    root_path = os.path.dirname(os.path.abspath(current_module.__file__))
    for root, dirs, _ in os.walk(root_path):
        search_paths = [os.path.join(root, d) for d in dirs] + [root]
        for module in pkgutil.iter_modules(search_paths):
            if module.name == module_name:
                spec = importlib.util.spec_from_file_location(
                    module_name,
                    os.path.join(module.module_finder.path,
                                 module_name, '__init__.py')
                )
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod


class ModelValidationError(Exception):
    """
    Exception raised when one or more validation errors occur in the model.

    Attributes:
        errors (list): A list of error messages returned from validation methods.
    """

    def __init__(self, errors):
        # Ensure errors is a list of messages
        if isinstance(errors, str):
            errors = [errors]  # Convert a single string to a list
        elif not isinstance(errors, list):
            raise ValueError("Errors should be a string or a list of strings")
        self.errors = errors
        # Call the base class constructor with a formatted error message
        super().__init__(self.format_errors())

    def format_errors(self):
        # Format the error messages as a single string
        return "\n".join(self.errors)

    def __str__(self):
        # Return the formatted error messages
        return self.format_errors()


@dataclass(kw_only=True)
class VersionedModel:
    """A base class for versioned models with common (Big 6) attributes."""

    entity_id: str = field(default_factory=get_uuid_hex,
                           metadata={'field_type': 'entity_id'})
    version: str = field(default_factory=lambda: get_uuid_hex(
        0), metadata={'field_type': 'uuid'})
    previous_version: str = field(
        default_factory=lambda: None, metadata={'field_type': 'uuid'})
    active: bool = True
    changed_by_id: str = field(default_factory=lambda: get_uuid_hex(
        0), metadata={'field_type': 'uuid'})
    changed_on: datetime = field(default_factory=default_datetime)
    extra: Dict[str, Any] = field(default_factory=dict)

    _is_partial: InitVar[bool] = False

    def __post_init__(self, _is_partial):
        """
        Post-initialization hook for the VersionedModel class.

        This method is responsible for setting the `_is_partial` attribute,
        resolving related model classes from strings specified in field metadata,
        and handling fields that are lists of UUIDs.

        Args:
            _is_partial (bool): Indicates if the instance is a partial representation
                                (e.g., loaded as a foreign key reference).

        Raises:
            ImportError: If a related model class specified as a string cannot be imported.
        """
        self._is_partial = _is_partial
        current_module = importlib.import_module('__main__')
        models_module = import_models_module(current_module, 'models')
        rococo_module = importlib.import_module('rococo.models')

        for f in fields(self):
            # Resolve and load related model classes if specified as string in metadata
            metadata = f.metadata.get('relationship', {})
            model_name = metadata.get('model')
            if isinstance(model_name, str):
                field_model_cls = (
                    getattr(current_module, model_name, None)
                    or (getattr(models_module, model_name, None) if models_module else None)
                    or (getattr(rococo_module, model_name, None) if rococo_module else None)
                )
                if not field_model_cls:
                    raise ImportError(
                        f"Unable to import {model_name} class from current/module/models.")
                metadata['model'] = field_model_cls

            # Handle list of uuid.UUID specifically
            hint = get_type_hints(self.__class__).get(f.name)
            if getattr(hint, '__origin__', None) is list and UUID in getattr(hint, '__args__', []):
                value = getattr(self, f.name)
                if value is None:
                    setattr(self, f.name, [])
                elif isinstance(value, str):
                    try:
                        uuid_list = [UUID(u.strip())
                                     for u in value[1:-1].split(',') if u.strip()]
                        setattr(self, f.name, uuid_list)
                    except ValueError:
                        logger.info(
                            f"Invalid UUIDs in list for field '{f.name}'")

    def __getattribute__(self, name):
        """
        Override of __getattribute__ to add additional checks for partial instances
        and many-to-many fields.

        For partial instances (i.e. instances loaded from the database as a
        foreign key of another model), this method raises an AttributeError if
        the requested attribute is not the 'entity_id' field.

        For many-to-many fields, this method raises an AttributeError if the
        field has not been loaded yet (i.e. its value is None).
        """
        # Skip checks for private attributes and special methods to avoid recursion
        if name.startswith('_') or name in ['fields']:
            return object.__getattribute__(self, name)

        # Handle partial instance restrictions first
        try:
            is_partial = object.__getattribute__(self, '_is_partial')
        except AttributeError:
            # _is_partial not set yet, continue normally
            pass

        if is_partial and name in VersionedModel.fields() and name != 'entity_id':
            raise AttributeError(
                f"Attribute '{name}' is not available in a partial instance.")

        # Check for m2m field restrictions
        try:
            f = next((f for f in fields(type(self)) if f.name == name), None)
            if f and f.metadata.get('field_type') == 'm2m_list':
                # Try to get the value using object.__getattribute__
                try:
                    value = object.__getattribute__(self, name)
                    if value is None:
                        raise AttributeError(
                            f"Many-to-many field '{name}' is not loaded.")
                    return value
                except AttributeError:
                    # Field doesn't exist, so it's definitely not loaded
                    raise AttributeError(
                        f"Many-to-many field '{name}' is not loaded.")
        except (TypeError, StopIteration):
            # fields() might not be available during initialization, continue normally
            pass

        # Get the value normally
        return object.__getattribute__(self, name)

    def __getattr__(self, name):
        """
        Called when an attribute is not found through normal lookup.
        This handles extra fields for models that allow them.
        """

        f = next((f for f in fields(type(self)) if f.name == name), None)
        if f and f.metadata.get('field_type') == 'm2m_list':
            raise AttributeError(
                f"Many-to-many field '{name}' is not loaded.")

        # Handle partial instance restrictions first
        try:
            is_partial = object.__getattribute__(self, '_is_partial')
        except AttributeError:
            # _is_partial not set yet, continue normally
            pass

        if is_partial and name in VersionedModel.fields() and name != 'entity_id':
            raise AttributeError(
                f"Attribute '{name}' is not available in a partial instance.")

        try:
            extra = object.__getattribute__(self, 'extra')
        except AttributeError:
            # extra not initialized yet
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'")

        # Check if the field is directly in extra
        if name in extra:
            return extra[name]

        # Check if the field is nested under 'extra' key (for backward compatibility)
        if 'extra' in extra and isinstance(extra['extra'], dict) and name in extra['extra']:
            return extra['extra'][name]

        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value):
        """
        Allow setting extra fields directly as attributes.
        If the field is not a defined model field and allow_extra is True,
        store it in the extra dict.
        Skip setting calculated properties (properties without setters).
        """
        # Check if this is a calculated property (property without setter)
        if not name.startswith('_'):
            attr = getattr(type(self), name, None)
            if isinstance(attr, property) and attr.fset is None:
                # This is a calculated property (read-only), skip setting it
                return

        # Get model fields (but handle the case where fields() might not be available yet)
        try:
            model_fields = self.fields()
        except:
            # During initialization, fields() might not work yet
            model_fields = [f.name for f in fields(type(self))]

        # Always allow setting defined model fields and private attributes
        if name.startswith('_') or name in model_fields:
            object.__setattr__(self, name, value)
        elif getattr(type(self), 'allow_extra', False) and name != 'extra':
            # Initialize extra dict if it doesn't exist
            try:
                extra = object.__getattribute__(self, 'extra')
            except AttributeError:
                object.__setattr__(self, 'extra', {})
                extra = object.__getattribute__(self, 'extra')
            extra[name] = value
        else:
            # Default behavior - set the attribute normally
            object.__setattr__(self, name, value)

    def __repr__(self) -> str:
        """
        Return a string representation of the VersionedModel instance.

        This method generates a string that includes the class name and a list of
        field names with their respective values. For partial instances, only the
        `entity_id` and `_is_partial` flag are included. For fields marked as 
        many-to-many lists, it displays a placeholder `[...]` to indicate the 
        presence of related entities. Any unloaded fields are represented with 
        `<unloaded>`.

        Returns:
            str: A string representation of the instance.
        """
        if self._is_partial:
            return f"{type(self).__name__}(entity_id={self.entity_id!r}, _is_partial=True)"

        field_strings = []
        for f in fields(type(self)):
            try:
                value = getattr(self, f.name)
                if f.metadata.get('field_type') == 'm2m_list' and value is not None:
                    field_strings.append(f"{f.name}=[...]")
                else:
                    field_strings.append(f"{f.name}={repr(value)}")
            except AttributeError:
                field_strings.append(f"{f.name}=<unloaded>")

        return f"{type(self).__name__}({', '.join(field_strings)})"

    @classmethod
    def fields(cls) -> List[str]:
        """
        Get a list of field names for this model.

        Returns:
            List[str]: A list of field names.
        """
        return [f.name for f in fields(cls) if f.name != 'extra']

    def as_dict(self, convert_datetime_to_iso_string: bool = False, convert_uuids: bool = True, export_properties: bool = True) -> Dict[str, Any]:
        """
        Convert this model to a dictionary.

        Args:
            convert_datetime_to_iso_string (bool, optional): Whether to convert datetime to ISO strings.
            convert_uuids (bool): Whether to convert UUIDs to strings.
            export_properties (bool): Whether to include @property methods in the output.

        Returns:
            Dict[str, Any]: A dictionary representation of this model.
        """
        if self._is_partial:
            return {'entity_id': self.entity_id}

        result = {k: v for k, v in self.__dict__.items() if k in self.fields()}
        keys_to_remove = []

        for k, v in result.items():
            f = next((f for f in fields(type(self)) if f.name == k), None)

            if f.metadata.get('field_type') == 'm2m_list':
                if v is None:
                    keys_to_remove.append(k)
                elif isinstance(v, list):
                    result[k] = [obj.as_dict(
                        convert_datetime_to_iso_string) for obj in v]

            elif f.metadata.get('field_type') in ['record_id', 'entity_id']:
                # Handle references or nested versioned models
                if isinstance(v, VersionedModel):
                    result[k] = {'entity_id': str(v.entity_id)} if v._is_partial else v.as_dict(
                        convert_datetime_to_iso_string)
                elif isinstance(v, UUID):
                    result[k] = str(v) if convert_uuids else v
                elif isinstance(v, list) and all(isinstance(i, UUID) for i in v):
                    result[k] = [str(i) if convert_uuids else i for i in v]
                elif is_dataclass(v):
                    result[k] = v.as_dict(
                        convert_datetime_to_iso_string, convert_uuids) if hasattr(v, 'as_dict') else v
                elif isinstance(v, dict):
                    result[k] = str(v.get('entity_id')
                                    ) if convert_uuids else v.get('entity_id')

            if convert_datetime_to_iso_string and isinstance(v, datetime):
                result[k] = v.isoformat()
            if convert_uuids and isinstance(v, UUID):
                result[k] = str(v)
            # Convert enum values to their string representation
            if isinstance(v, Enum):
                result[k] = v.value

            # Convert dataclass fields with 'model' metadata
            f = next((f for f in fields(type(self)) if f.name == k), None)
            if f and f.metadata.get('model') and v is not None:
                model_class = f.metadata['model']
                if isinstance(v, list):
                    # Handle list of dataclass objects
                    result[k] = [
                        obj.__dict__ if is_dataclass(obj) else obj
                        for obj in v
                    ]
                elif is_dataclass(v):
                    # Handle single dataclass object
                    result[k] = v.__dict__

        for k in keys_to_remove:
            result.pop(k, None)

        # Handle extra fields - unwrap them into the result dict
        if hasattr(self, 'extra') and self.extra:
            for extra_key, extra_value in self.extra.items():
                result[extra_key] = extra_value

        # Remove the 'extra' field itself from the result
        result.pop('extra', None)

        # Export properties if requested
        if export_properties:
            for attr_name in dir(type(self)):
                # Skip private attributes and methods
                if attr_name.startswith('_'):
                    continue

                # Get the attribute from the class
                attr = getattr(type(self), attr_name, None)

                # Check if it's a property
                if isinstance(attr, property):
                    # Skip if it's already in the result (from regular fields)
                    if attr_name not in result:
                        try:
                            # Get the property value
                            prop_value = getattr(self, attr_name)

                            # Apply the same conversions as regular fields
                            if convert_datetime_to_iso_string and isinstance(prop_value, datetime):
                                prop_value = prop_value.isoformat()
                            if convert_uuids and isinstance(prop_value, UUID):
                                prop_value = str(prop_value)
                            if isinstance(prop_value, Enum):
                                prop_value = prop_value.value

                            result[attr_name] = prop_value
                        except Exception:
                            # Skip properties that raise exceptions when accessed
                            pass

        # Apply field aliases for serialization (only for custom fields, not Big 6)
        aliased_result = {}
        for k, v in result.items():
            if k not in BIG_6_FIELDS:
                f = next((f for f in fields(type(self)) if f.name == k), None)
                if f and f.metadata.get('alias'):
                    # Use alias as the key in the output
                    aliased_result[f.metadata['alias']] = v
                else:
                    # Use original field name
                    aliased_result[k] = v
            else:
                # Big 6 fields always use original names
                aliased_result[k] = v

        return aliased_result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedModel":
        """
        Load VersionedModel from dict
        """
        # Handle field aliases for deserialization (only for custom fields, not Big 6)
        # Create a mapping from alias to field name for custom fields
        alias_to_field = {}
        for f in fields(cls):
            if f.name not in BIG_6_FIELDS and f.metadata.get('alias'):
                alias_to_field[f.metadata['alias']] = f.name

        # Convert aliased keys back to field names
        converted_data = {}
        for k, v in data.items():
            if k in alias_to_field:
                # Use the original field name
                converted_data[alias_to_field[k]] = v
            else:
                # Use the key as-is
                converted_data[k] = v

        clean_data = {k: v for k, v in converted_data.items()
                      if k in cls.fields()}
        hints = get_type_hints(cls)

        for k, v in clean_data.items():
            if k in BIG_6_UUID_FIELDS:
                try:
                    clean_data[k] = UUID(
                        v).hex if v and not isinstance(v, UUID) else v
                except ValueError:
                    logger.info(f"'{v}' is not a valid UUID.")

            # Handle enum conversion from string values and datetime conversion from ISO strings
            elif v is not None:
                expected_type = hints.get(k)
                if expected_type:
                    # Handle Optional[EnumType] (Union[EnumType, None])
                    origin = get_origin(expected_type)
                    if origin is Union:
                        args = get_args(expected_type)
                        # Find the non-None type in the Union
                        enum_type = next((arg for arg in args if arg is not type(
                            None) and isinstance(arg, type) and issubclass(arg, Enum)), None)
                        if enum_type and isinstance(v, str):
                            try:
                                clean_data[k] = enum_type(v)
                            except ValueError:
                                # If the string value doesn't match any enum value, leave as is
                                pass
                        # Find datetime type in the Union for Optional[datetime]
                        datetime_type = next(
                            (arg for arg in args if arg is datetime), None)
                        if datetime_type and isinstance(v, str):
                            try:
                                clean_data[k] = isoparse(v)
                            except (ValueError, TypeError):
                                # If the string value can't be parsed as datetime, leave as is
                                pass
                    # Handle direct enum types
                    elif isinstance(expected_type, type) and issubclass(expected_type, Enum) and isinstance(v, str):
                        try:
                            clean_data[k] = expected_type(v)
                        except ValueError:
                            # If the string value doesn't match any enum value, leave as is
                            pass
                    # Handle direct datetime types
                    elif expected_type is datetime and isinstance(v, str):
                        try:
                            clean_data[k] = isoparse(v)
                        except (ValueError, TypeError):
                            # If the string value can't be parsed as datetime, leave as is
                            pass

                # Handle dataclass conversion from dict values
                f = next((f for f in fields(cls) if f.name == k), None)
                if f and f.metadata.get('model') and isinstance(v, (dict, list)):
                    model_class = f.metadata['model']
                    if isinstance(v, list):
                        # Handle list of dict objects -> list of dataclass objects
                        clean_data[k] = [
                            model_class(**item) if isinstance(item,
                                                              dict) else item
                            for item in v
                        ]
                    elif isinstance(v, dict):
                        # Handle single dict object -> dataclass object
                        clean_data[k] = model_class(**v)

        # Handle extra fields if the model allows them
        extra_data = {}
        if getattr(cls, 'allow_extra', False):
            # Collect fields that are not in the model definition
            model_fields = cls.fields()
            for k, v in data.items():
                if k not in model_fields and k != 'extra':  # Don't include 'extra' itself
                    extra_data[k] = v

            # Also handle explicit 'extra' field from data
            if 'extra' in data and isinstance(data['extra'], dict):
                extra_data.update(data['extra'])

        # Create the instance
        instance = cls(**clean_data)

        # Set extra fields directly (not nested), but skip calculated properties
        if extra_data:
            filtered_extra_data = {}
            for k, v in extra_data.items():
                # Check if this is a calculated property (property without setter)
                attr = getattr(cls, k, None)
                if isinstance(attr, property) and attr.fset is None:
                    # This is a calculated property (read-only), skip setting it
                    continue
                filtered_extra_data[k] = v

            # Replace the entire extra dict to avoid nesting
            instance.extra = filtered_extra_data

        return instance

    def validate(self):
        """
        Validate all fields by calling corresponding `validate_<field_name>` methods if defined,
        and validate the type of each field. Raise `ModelValidationError` if any validations fail.
        """
        errors = []
        hints = get_type_hints(type(self))
        castable = {int, str, float, bool, UUID}

        for name in self.fields():
            value = getattr(self, name)
            expected = hints.get(name)
            validator = getattr(self, f"validate_{name}", None)
            if callable(validator):
                error = validator()
                if error:
                    errors.append(error)

            if not getattr(type(self), 'use_type_checking', False):
                continue

            origin = get_origin(expected)
            args = get_args(expected)

            if origin is Union:
                if value is None and type(None) in args:
                    continue
                for arg in sorted(args, key=lambda x: 0 if x in castable else 1):
                    try:
                        if isinstance(value, arg):
                            break
                        if arg in castable or issubclass(arg, Enum):
                            setattr(self, name, arg(value))
                            break
                    except Exception:
                        continue
                else:
                    errors.append(
                        f"Invalid type for '{name}': expected {args}, got {type(value).__name__}")
            elif value is None:
                errors.append(
                    f"Invalid type for '{name}': expected {expected.__name__}, got NoneType")
            elif not isinstance(value, expected):
                try:
                    if expected in castable or issubclass(expected, Enum):
                        setattr(self, name, expected(value))
                    elif isinstance(value, UUID) and expected is str:
                        setattr(self, name, value.hex)
                    else:
                        raise TypeError
                except Exception:
                    errors.append(
                        f"Invalid type for '{name}': expected {expected.__name__}, got {type(value).__name__}")

        if errors:
            raise ModelValidationError(errors)

    def prepare_for_save(self, changed_by_id: UUID):
        """
        Prepare this model for saving to the database.

        Args:
            changed_by_id (str): The ID of the user making the change.
        """
        if not self.entity_id:
            self.entity_id = get_uuid_hex()
        if self.version:
            self.previous_version = self.version
        else:
            self.previous_version = get_uuid_hex(0)
        self.version = get_uuid_hex()
        self.changed_on = datetime.now(timezone.utc)

        if changed_by_id:
            self.changed_by_id = changed_by_id
        self.validate()
