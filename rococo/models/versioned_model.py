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

    _is_partial: InitVar[bool] = field(default=False)  # noqa: E501

    def _resolve_model_class(self, model_name: str, current_module, models_module, rococo_module):
        """Resolve a model class by name from available modules."""
        field_model_cls = getattr(current_module, model_name, None)
        if not field_model_cls and models_module:
            field_model_cls = getattr(models_module, model_name, None)
        if not field_model_cls and rococo_module:
            field_model_cls = getattr(rococo_module, model_name, None)
        return field_model_cls

    def _handle_uuid_list_field(self, f):
        """Handle initialization of UUID list fields."""
        hint = get_type_hints(self.__class__).get(f.name)
        is_uuid_list = (getattr(hint, '__origin__', None) is list and
                        UUID in getattr(hint, '__args__', []))
        if not is_uuid_list:
            return

        value = getattr(self, f.name)
        if value is None:
            setattr(self, f.name, [])
        elif isinstance(value, str):
            self._parse_uuid_string_to_list(f.name, value)

    def _parse_uuid_string_to_list(self, field_name: str, value: str):
        """Parse a string representation of UUIDs into a list."""
        try:
            uuid_list = [UUID(u.strip()) for u in value[1:-1].split(',') if u.strip()]
            setattr(self, field_name, uuid_list)
        except ValueError:
            logger.info(f"Invalid UUIDs in list for field '{field_name}'")

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
            self._resolve_field_model(f, current_module, models_module, rococo_module)
            self._handle_uuid_list_field(f)

    def _resolve_field_model(self, f, current_module, models_module, rococo_module):
        """Resolve and load related model classes if specified as string in metadata."""
        metadata = f.metadata.get('relationship', {})
        model_name = metadata.get('model')
        if not isinstance(model_name, str):
            return

        field_model_cls = self._resolve_model_class(
            model_name, current_module, models_module, rococo_module)
        if not field_model_cls:
            raise ImportError(
                f"Unable to import {model_name} class from current/module/models.")
        metadata['model'] = field_model_cls

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
        except (TypeError, AttributeError):
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

    def _convert_value_for_dict(self, v, convert_datetime_to_iso_string: bool, convert_uuids: bool):
        """Convert a value for dictionary output."""
        if convert_datetime_to_iso_string and isinstance(v, datetime):
            return v.isoformat()
        if convert_uuids and isinstance(v, UUID):
            return str(v)
        if isinstance(v, Enum):
            return v.value
        return v

    def _convert_m2m_field(self, v, convert_datetime_to_iso_string: bool):
        """Convert many-to-many list field."""
        if v is None:
            return None  # Signal to remove
        if isinstance(v, list):
            return [obj.as_dict(convert_datetime_to_iso_string) for obj in v]
        return v

    def _convert_reference_field(self, v, convert_datetime_to_iso_string: bool, convert_uuids: bool):
        """Convert record_id or entity_id field."""
        if isinstance(v, VersionedModel):
            return {'entity_id': str(v.entity_id)} if v._is_partial else v.as_dict(convert_datetime_to_iso_string)
        if isinstance(v, UUID):
            return str(v) if convert_uuids else v
        if isinstance(v, list) and v and all(isinstance(i, UUID) for i in v):
            return [str(i) if convert_uuids else i for i in v]
        if is_dataclass(v) and hasattr(v, 'as_dict'):
            return v.as_dict(convert_datetime_to_iso_string, convert_uuids)
        if isinstance(v, dict):
            return str(v.get('entity_id')) if convert_uuids else v.get('entity_id')
        return v

    def _convert_model_field(self, v):
        """Convert dataclass fields with 'model' metadata."""
        if v is None:
            return v
        if isinstance(v, list):
            return [obj.__dict__ if is_dataclass(obj) else obj for obj in v]
        if is_dataclass(v):
            return v.__dict__
        return v

    def _process_field_for_dict(self, v, f, convert_datetime_to_iso_string: bool, convert_uuids: bool):
        """Process a single field for dictionary conversion."""
        field_type = f.metadata.get('field_type') if f else None

        if field_type == 'm2m_list':
            return self._convert_m2m_field(v, convert_datetime_to_iso_string)
        if field_type in ['record_id', 'entity_id']:
            v = self._convert_reference_field(v, convert_datetime_to_iso_string, convert_uuids)

        v = self._convert_value_for_dict(v, convert_datetime_to_iso_string, convert_uuids)

        if f and f.metadata.get('model') and v is not None:
            v = self._convert_model_field(v)
        return v

    def _export_properties_to_dict(self, result: Dict, convert_datetime_to_iso_string: bool, convert_uuids: bool):
        """Export @property methods to dictionary."""
        for attr_name in dir(type(self)):
            if attr_name.startswith('_') or attr_name in result:
                continue
            attr = getattr(type(self), attr_name, None)
            if not isinstance(attr, property):
                continue
            try:
                prop_value = getattr(self, attr_name)
                result[attr_name] = self._convert_value_for_dict(
                    prop_value, convert_datetime_to_iso_string, convert_uuids)
            except Exception:
                pass  # Skip properties that raise exceptions

    def _apply_field_aliases(self, result: Dict) -> Dict:
        """Apply field aliases for serialization."""
        aliased_result = {}
        for k, v in result.items():
            if k in BIG_6_FIELDS:
                aliased_result[k] = v
                continue
            f = next((f for f in fields(type(self)) if f.name == k), None)
            alias = f.metadata.get('alias') if f else None
            aliased_result[alias if alias else k] = v
        return aliased_result

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

        for k, v in result.copy().items():
            f = next((f for f in fields(type(self)) if f.name == k), None)
            converted = self._process_field_for_dict(v, f, convert_datetime_to_iso_string, convert_uuids)
            if converted is None and f and f.metadata.get('field_type') == 'm2m_list':
                keys_to_remove.append(k)
            else:
                result[k] = converted

        for k in keys_to_remove:
            result.pop(k, None)

        if hasattr(self, 'extra') and self.extra:
            result.update(self.extra)
        result.pop('extra', None)

        if export_properties:
            self._export_properties_to_dict(result, convert_datetime_to_iso_string, convert_uuids)

        return self._apply_field_aliases(result)

    @classmethod
    def _build_alias_mapping(cls) -> Dict[str, str]:
        """Build a mapping from field aliases to field names."""
        return {f.metadata['alias']: f.name for f in fields(cls)
                if f.name not in BIG_6_FIELDS and f.metadata.get('alias')}

    @classmethod
    def _convert_aliased_data(cls, data: Dict[str, Any], alias_to_field: Dict[str, str]) -> Dict[str, Any]:
        """Convert aliased keys back to field names."""
        return {alias_to_field.get(k, k): v for k, v in data.items()}

    @classmethod
    def _convert_uuid_field(cls, k: str, v) -> Any:
        """Convert a UUID field value."""
        if not v or isinstance(v, UUID):
            return v
        try:
            return UUID(v).hex
        except ValueError:
            logger.info(f"'{v}' is not a valid UUID.")
            return v

    @classmethod
    def _convert_enum_or_datetime(cls, v, expected_type) -> Any:
        """Convert string values to enum or datetime types."""
        if not isinstance(v, str):
            return v

        origin = get_origin(expected_type)
        if origin is Union:
            return cls._convert_union_type(v, get_args(expected_type))

        if isinstance(expected_type, type) and issubclass(expected_type, Enum):
            return cls._try_convert_enum(v, expected_type)

        if expected_type is datetime:
            return cls._try_convert_datetime(v)

        return v

    @classmethod
    def _convert_union_type(cls, v: str, args) -> Any:
        """Convert value for Union/Optional types."""
        for arg in args:
            if arg is type(None):
                continue
            if isinstance(arg, type) and issubclass(arg, Enum):
                result = cls._try_convert_enum(v, arg)
                if result != v:
                    return result
            if arg is datetime:
                result = cls._try_convert_datetime(v)
                if result != v:
                    return result
        return v

    @classmethod
    def _try_convert_enum(cls, v: str, enum_type) -> Any:
        """Try to convert a string to an enum value."""
        try:
            return enum_type(v)
        except ValueError:
            return v

    @classmethod
    def _try_convert_datetime(cls, v: str) -> Any:
        """Try to convert a string to a datetime value."""
        try:
            return isoparse(v)
        except (ValueError, TypeError):
            return v

    @classmethod
    def _convert_model_from_dict(cls, v, model_class) -> Any:
        """Convert dict/list values to model instances."""
        if isinstance(v, list):
            return [model_class(**item) if isinstance(item, dict) else item for item in v]
        if isinstance(v, dict):
            return model_class(**v)
        return v

    @classmethod
    def _collect_extra_data(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Collect extra fields not in the model definition."""
        model_fields = cls.fields()
        extra_data = {k: v for k, v in data.items() if k not in model_fields and k != 'extra'}
        if 'extra' in data and isinstance(data['extra'], dict):
            extra_data.update(data['extra'])
        return extra_data

    @classmethod
    def _filter_extra_data(cls, extra_data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out read-only properties from extra data."""
        return {k: v for k, v in extra_data.items()
                if not (isinstance(getattr(cls, k, None), property) and
                        getattr(cls, k).fset is None)}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedModel":
        """
        Load VersionedModel from dict
        """
        alias_to_field = cls._build_alias_mapping()
        converted_data = cls._convert_aliased_data(data, alias_to_field)
        clean_data = {k: v for k, v in converted_data.items() if k in cls.fields()}
        hints = get_type_hints(cls)

        for k, v in clean_data.items():
            if k in BIG_6_UUID_FIELDS:
                clean_data[k] = cls._convert_uuid_field(k, v)
            elif v is not None:
                expected_type = hints.get(k)
                if expected_type:
                    clean_data[k] = cls._convert_enum_or_datetime(v, expected_type)

                f = next((f for f in fields(cls) if f.name == k), None)
                if f and f.metadata.get('model') and isinstance(v, (dict, list)):
                    clean_data[k] = cls._convert_model_from_dict(v, f.metadata['model'])

        instance = cls(**clean_data)

        if getattr(cls, 'allow_extra', False):
            extra_data = cls._collect_extra_data(data)
            if extra_data:
                instance.extra = cls._filter_extra_data(extra_data)

        return instance

    def _run_field_validator(self, name: str, errors: list):
        """Run custom field validator if defined."""
        validator = getattr(self, f"validate_{name}", None)
        if callable(validator):
            error = validator()
            if error:
                errors.append(error)

    def _validate_union_type(self, name: str, value, args, castable: set, errors: list):
        """Validate a field with Union type."""
        if value is None and type(None) in args:
            return
        for arg in sorted(args, key=lambda x: 0 if x in castable else 1):
            if self._try_cast_value(name, value, arg, castable):
                return
        errors.append(f"Invalid type for '{name}': expected {args}, got {type(value).__name__}")

    def _try_cast_value(self, name: str, value, arg, castable: set) -> bool:
        """Try to cast a value to the specified type."""
        try:
            if isinstance(value, arg):
                return True
            if arg in castable or (isinstance(arg, type) and issubclass(arg, Enum)):
                setattr(self, name, arg(value))
                return True
        except Exception:
            pass
        return False

    def _validate_simple_type(self, name: str, value, expected, castable: set, errors: list):
        """Validate a field with a simple (non-Union) type."""
        if value is None:
            errors.append(f"Invalid type for '{name}': expected {expected.__name__}, got NoneType")
            return
        if isinstance(value, expected):
            return
        if self._try_convert_simple_type(name, value, expected, castable):
            return
        errors.append(f"Invalid type for '{name}': expected {expected.__name__}, got {type(value).__name__}")

    def _try_convert_simple_type(self, name: str, value, expected, castable: set) -> bool:
        """Try to convert a value to the expected simple type."""
        try:
            if expected in castable or issubclass(expected, Enum):
                setattr(self, name, expected(value))
                return True
            if isinstance(value, UUID) and expected is str:
                setattr(self, name, value.hex)
                return True
        except Exception:
            pass
        return False

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
            self._run_field_validator(name, errors)

            if not getattr(type(self), 'use_type_checking', False):
                continue

            origin = get_origin(expected)
            if origin is Union:
                self._validate_union_type(name, value, get_args(expected), castable, errors)
            else:
                self._validate_simple_type(name, value, expected, castable, errors)

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
