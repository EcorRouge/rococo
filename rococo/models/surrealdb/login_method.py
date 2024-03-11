"""
LoginMethod model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import SurrealVersionedModel


@dataclass
class LoginMethod(SurrealVersionedModel):
    """A login method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    method_type: Optional[str] = None
    method_data: Optional[dict] = None
    email: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'Email', 'type': 'direct'},
        'field_type': 'record_id'
    })
    password: Optional[str] = None
