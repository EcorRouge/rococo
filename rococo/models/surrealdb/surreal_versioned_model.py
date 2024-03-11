import os
import pkgutil
import importlib
from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields
from rococo.models import VersionedModel


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
class SurrealVersionedModel(VersionedModel):
    """A base class for versioned models with common (Big 6) attributes specific to SurrealDB."""

    entity_id: UUID = field(default_factory=uuid4, metadata={'field_type': 'record_id'})

    def __post_init__(self, _is_partial):
        self._is_partial = _is_partial
        for field in fields(self):
            field_model = field.metadata.get('relationship', {}).get('model')
            if field_model is not None and isinstance(field_model, str):
                current_module = importlib.import_module('__main__')
                models_module = import_models_module(current_module, 'models')
                rococo_module = importlib.import_module('rococo.models.surrealdb')
                field_model_cls = getattr(current_module, field_model, None) or  \
                                    (getattr(models_module, field_model, None) if models_module else None) or \
                                    (getattr(rococo_module, field_model, None) if rococo_module else None)
                if not field_model_cls:
                    raise ImportError(f"Unable to import {field_model} class from current module or models module.")

                field.metadata['relationship']['model'] = field_model_cls

