"""
Email model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import SurrealVersionedModel

@dataclass
class Email(SurrealVersionedModel):
    """A email method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    email: Optional[str] = None
    is_verified: bool = False
    is_default: bool = False
