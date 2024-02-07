"""
Email model
"""

from dataclasses import dataclass, field
from rococo.models import BaseEmail
from rococo.models.surrealdb import VersionedModel

@dataclass
class Email(VersionedModel,BaseEmail):
    """A email method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
