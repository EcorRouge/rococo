"""
VersionedModel for rococo. Extends BaseVersionedModel
"""

import pkgutil
import os

from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar
from datetime import datetime
from typing import Any, Dict
import importlib
from rococo.models import BaseVersionedModel


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.utcnow()


def import_models_module(current_module, module_name):
    """Import models module"""
    root_path = os.path.dirname(os.path.abspath(current_module.__file__))

    for root, dirs, _ in os.walk(root_path):
        for module in pkgutil.iter_modules([os.path.join(root, dir) for dir in dirs] + [root]):
            if module.name == module_name:
                spec = importlib.util.spec_from_file_location(
                    module_name, os.path.join(
                        module.module_finder.path, module_name, '__init__.py'))
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module


@dataclass(kw_only=True)
class VersionedModel(BaseVersionedModel):
    """A base class for versioned models with common (Big 6) attributes."""

    entity_id: UUID = field(default_factory=uuid4, metadata={'field_type': 'record_id'})

    _is_partial: InitVar[bool] = False

    def __post_init__(self, _is_partial): # pylint: disable=W0221
        self._is_partial = _is_partial
        for field_ in fields(self):
            field_model = field_.metadata.get('relationship', {}).get('model')
            if field_model is not None and isinstance(field_model, str):
                current_module = importlib.import_module('__main__')
                models_module = import_models_module(current_module, 'models')
                rococo_module = importlib.import_module('rococo.models')
                field_model_cls = getattr(current_module, field_model, None) or  \
                                    (getattr(models_module, field_model, None
                                        ) if models_module else None) or \
                                    (getattr(rococo_module, field_model, None
                                        ) if rococo_module else None)
                if not field_model_cls:
                    raise ImportError(
                        f"Unable to import {field_model} class from "
                        f"current module or models module.")

                field_.metadata['relationship']['model'] = field_model_cls


    def __getattribute__(self, name):
        _field_names = [field for field in object.__getattribute__(self, 'fields')()]
        if name in _field_names:
            if object.__getattribute__(self, '_is_partial') is True and name != 'entity_id':
                raise AttributeError(
                    f"The object being accessed is not fetched from the database, "
                    f"and has no attributes available other than entity_id. Accessed: {name}"
                )

        _field = next(
            (field for field in fields(object.__getattribute__(self, '__class__')
                                       ) if field.name == name), None)
        field_value = object.__getattribute__(self, name)
        if _field and _field.metadata.get('field_type') == 'm2m_list' and field_value is None:
            raise AttributeError(
                f"The many-to-many list attribute being accessed is not fetched from the "
                f"database. Accessed: {name}"
            )

        return object.__getattribute__(self, name)

    def __repr__(self) -> str:
        if self._is_partial:
            return (
                f"{self.__class__.__name__}(entity_id={self.entity_id.__repr__()}, "
                "_is_partial=True)"
            )

        repr_string = f"{self.__class__.__name__}("
        repr_fields = []
        _fields = [field for field in fields(object.__getattribute__(self, '__class__'))]
        for f in _fields:
            if f.metadata.get('field_type') == 'm2m_list':
                repr_fields.append(
                    f"{f.name}=None" if object.__getattribute__(
                        self, f.name) is None else f"{f.name}=[...]")
            else:
                repr_fields.append(f"{f.name}={getattr(self, f.name).__repr__()}")

        repr_string += ", ".join(repr_fields) + ")"
        return repr_string

    def as_dict(self, convert_datetime_to_iso_string: bool = False) -> Dict[str, Any]:
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
            _field = next(
                (field for field in fields(object.__getattribute__(self, '__class__')
                                           ) if field.name == key), None)
            if _field.metadata.get('field_type') == 'm2m_list':
                if value is None:
                    keys_to_pop.append(_field.name)
                elif isinstance(value, list):
                    results[key] = [obj.as_dict(convert_datetime_to_iso_string) for obj in value]
                else:
                    raise NotImplementedError

            if _field.metadata.get('field_type') == 'record_id':
                if value is None:
                    results[key] = None
                elif isinstance(value, VersionedModel):
                    if value._is_partial: # pylint: disable=W0212
                        results[key] = {'entity_id': str(value.entity_id)}
                    else:
                        results[key] = value.as_dict(convert_datetime_to_iso_string)
                elif isinstance(value, UUID):
                    results[key] = str(value)
                elif isinstance(value, str):
                    results[key] = value
                else:
                    raise NotImplementedError

            if convert_datetime_to_iso_string:
                if isinstance(value, datetime):
                    results[key] = value.isoformat()
            if isinstance(value,UUID):
                results[key] = str(value)

        for key in keys_to_pop:
            results.pop(key, None)
        return results
