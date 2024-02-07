"""
LoginMethod model
"""

from dataclasses import dataclass, field
from typing import Optional

from rococo.models import BaseLoginMethod
from rococo.models.surrealdb import VersionedModel

@dataclass
class LoginMethod(VersionedModel, BaseLoginMethod):
    """A login method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    email: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'Email', 'type': 'direct'},
        'field_type': 'record_id'
    })
