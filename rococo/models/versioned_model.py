"""
VersionedModel for rococo. Base model
"""

import pkgutil
import os

from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar
from datetime import datetime
from typing import Any, Dict, List
import importlib


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.utcnow()


def import_models_module(current_module, module_name):
    root_path = os.path.dirname(os.path.abspath(current_module.__file__))

    for root, dirs, _ in os.walk(root_path):
        for module in pkgutil.iter_modules([os.path.join(root, dir) for dir in dirs] + [root]):
            if module.name == module_name:
                spec = importlib.util.spec_from_file_location(module_name, os.path.join(module.module_finder.path, module_name, '__init__.py'))
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module


@dataclass(kw_only=True)
class VersionedModel:
    """A base class for versioned models with common (Big 6) attributes."""

    entity_id: UUID = field(default_factory=uuid4, metadata={'field_type': 'entity_id'})
    version: UUID = UUID('00000000-0000-4000-8000-000000000000')
    previous_version: UUID = None
    active: bool = True
    changed_by_id: UUID = UUID('00000000-0000-4000-8000-000000000000')
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
                        filtered_data[k] = UUID(v)
                    except ValueError:
                        # Handle the case where the string is not a valid UUID
                        print(f"'{v}' is not a valid UUID.")
        return cls(**filtered_data)

    def prepare_for_save(self, changed_by_id: UUID):
        """
        Prepare this model for saving to the database.

        Args:
            changed_by_id (str): The ID of the user making the change.
        """
        if not self.entity_id:
            self.entity_id = uuid4()
        if self.version:
            self.previous_version = self.version
        else:
            self.previous_version = UUID('00000000-0000-4000-8000-000000000000')
        self.version = uuid4()
        self.changed_on = datetime.utcnow()
        if changed_by_id is not None:
            self.changed_by_id = changed_by_id
