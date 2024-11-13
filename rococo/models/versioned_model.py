"""
VersionedModel for rococo. Base model
"""

import pkgutil
import os

from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar
from datetime import datetime
from typing import Any, Dict, List, Union, get_type_hints, get_origin, get_args
import importlib
from enum import Enum


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.utcnow()


def get_uuid_hex(_int=None):
    if _int is None:
        return uuid4().hex
    else:
        return UUID(int=_int, version=4).hex


def import_models_module(current_module, module_name):
    root_path = os.path.dirname(os.path.abspath(current_module.__file__))

    for root, dirs, _ in os.walk(root_path):
        for module in pkgutil.iter_modules([os.path.join(root, dir) for dir in dirs] + [root]):
            if module.name == module_name:
                spec = importlib.util.spec_from_file_location(module_name, os.path.join(module.module_finder.path, module_name, '__init__.py'))
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module


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

    entity_id: str = field(default_factory=get_uuid_hex, metadata={'field_type': 'entity_id'})
    version: str = field(default_factory=lambda: get_uuid_hex(0), metadata={'field_type': 'uuid'})
    previous_version: str = field(default_factory=lambda: None, metadata={'field_type': 'uuid'})
    active: bool = True
    changed_by_id: str = field(default_factory=lambda: get_uuid_hex(0), metadata={'field_type': 'uuid'})
    changed_on: datetime = field(default_factory=default_datetime)

    _is_partial: InitVar[bool] = False

    def __post_init__(self, _is_partial):
        self._is_partial = _is_partial
        for field in fields(self):
            field_model = field.metadata.get('relationship', {}).get('model')
            if field_model is not None and isinstance(field_model, str):
                current_module = importlib.import_module('__main__')
                models_module = import_models_module(current_module, 'models')
                rococo_module = importlib.import_module('rococo.models')
                field_model_cls = getattr(current_module, field_model, None) or  \
                                    (getattr(models_module, field_model, None) if models_module else None) or \
                                    (getattr(rococo_module, field_model, None) if rococo_module else None)
                if not field_model_cls:
                    raise ImportError(f"Unable to import {field_model} class from current module or models module.")

                field.metadata['relationship']['model'] = field_model_cls


    def __getattribute__(self, name):
        _field_names = [field for field in object.__getattribute__(self, 'fields')()]
        if name in _field_names:
            if object.__getattribute__(self, '_is_partial') == True and name != 'entity_id':
                raise AttributeError(
                    f"The object being accessed is not fetched from the database, "
                    f"and has no attributes available other than entity_id. Accessed: {name}"
                )

        _field = next((field for field in fields(object.__getattribute__(self, '__class__')) if field.name == name), None)
        field_value = object.__getattribute__(self, name)
        if _field and _field.metadata.get('field_type') == 'm2m_list' and field_value is None:
            raise AttributeError(
                f"The many-to-many list attribute being accessed is not fetched from the "
                f"database. Accessed: {name}"
            )

        return object.__getattribute__(self, name)

    def __repr__(self) -> str:
        if self._is_partial:
            return f"{self.__class__.__name__}(entity_id={self.entity_id.__repr__()}, _is_partial=True)"
        else:
            repr_string = f"{self.__class__.__name__}("
            repr_fields = []
            _fields = [field for field in fields(object.__getattribute__(self, '__class__'))]
            for f in _fields:
                if f.metadata.get('field_type') == 'm2m_list':
                    repr_fields.append(f"{f.name}=None" if object.__getattribute__(self, f.name) is None else f"{f.name}=[...]")
                else:
                    repr_fields.append(f"{f.name}={getattr(self, f.name).__repr__()}")
            
            repr_string += ", ".join(repr_fields) + ")"
            return repr_string

    @classmethod
    def fields(cls) -> List[str]:
        """Get a list of field names for this model.
        
        Returns:
            List[str]: A list of field names.
        """
        return [f.name for f in fields(cls)]

    def as_dict(self, convert_datetime_to_iso_string: bool = False, convert_uuids: bool = True) -> Dict[str, Any]:
        """Convert this model to a dictionary.

        Args:
            convert_datetime_to_iso_string (bool, optional): Whether to
              convert datetime objects to ISO strings.
            Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of this model.
        """
        if self._is_partial:
            return {'entity_id': self.entity_id}

        results = self.__dict__

        results = {k:v for k,v in results.items() if k in self.fields()}

        keys_to_pop = []
        for key, value in results.items():
            _field = next((field for field in fields(object.__getattribute__(self, '__class__')) if field.name == key), None)
            if _field.metadata.get('field_type') == 'm2m_list':
                if value is None:
                    keys_to_pop.append(_field.name)
                elif isinstance(value, list):
                    results[key] = [obj.as_dict(convert_datetime_to_iso_string) for obj in value]
                else:
                    raise NotImplementedError
            
            if _field.metadata.get('field_type') in ['record_id', 'entity_id']:
                if value is None:
                    results[key] = None
                elif isinstance(value, VersionedModel):
                    if value._is_partial:
                        results[key] = {'entity_id': str(value.entity_id) if convert_uuids else value.entity_id}
                    else:
                        results[key] = value.as_dict(convert_datetime_to_iso_string)
                elif isinstance(value, UUID):
                    if convert_uuids:
                        results[key] = str(value)
                elif isinstance(value, str):
                    results[key] = value
                elif isinstance(value, dict):
                    results[key] = str(value.get('entity_id')) if convert_uuids else value.get('entity_id')
                else:
                    raise NotImplementedError

            if convert_datetime_to_iso_string:
                if isinstance(value, datetime):
                    results[key] = value.isoformat()
            if convert_uuids:
                if isinstance(value,UUID):
                    results[key] = str(value)

        for key in keys_to_pop:
            results.pop(key, None)
        return results

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedModel":
        """
        Load VersionedModel from dict
        """
        filtered_data = {k: v for k, v in data.items() if k in cls.fields()}
        for k,v in filtered_data.items():
            if k in ["entity_id", "version", "previous_version", "changed_by_id"]:
                if v is not None and not isinstance(v, UUID):
                    try:
                        # Attempt to cast the string to a UUID
                        filtered_data[k] = UUID(v).hex
                    except ValueError:
                        # Handle the case where the string is not a valid UUID
                        print(f"'{v}' is not a valid UUID.")
        return cls(**filtered_data)


    def validate(self): 
        """
        Validate all fields by calling corresponding `validate_<field_name>` methods if defined,
        and validate the type of each field. Raise `ModelValidationError` if any validations fail.
        """
        errors = []

        # Get the type hints for the current model (fields and their types)
        type_hints = get_type_hints(self.__class__)

        # Iterate through all fields of the class
        for field_name in self.fields():
            expected_type = type_hints[field_name]

            # Dynamically construct the method name to validate the field
            validation_method_name = f"validate_{field_name}"

            # Check if the validation method exists in the class
            validation_method = getattr(self, validation_method_name, None)

            # Call the validation method if it exists
            if callable(validation_method):
                error_message = validation_method()
                if error_message:
                    errors.append(error_message)

            if getattr(self.__class__, 'use_type_checking', False):
                # Validate the type of the field
                field_value = getattr(self, field_name)
                origin = get_origin(expected_type)

                castable_types = {int, str, float, bool, UUID}

                # Handle Union types (e.g., Optional[int])
                if origin is Union:
                    args = get_args(expected_type)
                    # Sort such that castable types are tried first
                    args = sorted(args, key=lambda x: 0 if x in castable_types else 1)

                    if field_value is None:
                        if type(None) in args:
                            pass  # None is acceptable
                        else:
                            errors.append(
                                f"Invalid type for field '{field_name}': Expected one of {args}, got {type(field_value).__name__}"
                            )
                    else:
                        # Flag to track casting success
                        cast_successful = False
                        for arg_type in args:
                            if isinstance(field_value, arg_type):
                                cast_successful = True
                                break
                            elif arg_type in castable_types or issubclass(arg_type, Enum):
                                try:
                                    # Try casting to the castable type or enum
                                    new_value = arg_type(field_value)
                                    setattr(self, field_name, new_value)
                                    cast_successful = True
                                    break
                                except (TypeError, ValueError):
                                    continue
                        if not cast_successful:
                            errors.append(
                                f"Invalid type for field '{field_name}': Expected one of {[t.__name__ for t in args]}, got {type(field_value).__name__}"
                            )
                else:
                    # Non-Union types
                    if field_value is None:
                        errors.append(
                            f"Invalid type for field '{field_name}': Expected {expected_type.__name__}, got NoneType"
                        )
                    if not isinstance(field_value, expected_type):
                        if type(field_value) is UUID and expected_type is str:
                            new_value = field_value.hex
                            setattr(self, field_name, new_value)
                        elif expected_type in castable_types or issubclass(expected_type, Enum):
                            try:
                                new_value = expected_type(field_value)
                                setattr(self, field_name, new_value)
                            except (TypeError, ValueError):
                                errors.append(
                                    f"Invalid type for field '{field_name}': Expected {expected_type.__name__}, got {type(field_value).__name__}"
                                )
                        else:
                            errors.append(
                                f"Invalid type for field '{field_name}': Expected {expected_type.__name__}, got {type(field_value).__name__}"
                            )

        # If there are validation errors, raise ModelValidationError
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
        self.changed_on = datetime.utcnow()
        if changed_by_id is not None:
            self.changed_by_id = changed_by_id
        self.validate()
