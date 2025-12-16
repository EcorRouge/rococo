"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import SurrealVersionedModel


@dataclass
class PersonOrganizationRole(SurrealVersionedModel):
    """A person organization role model."""

    person: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    organization: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'Organization', 'type': 'direct'},
        'field_type': 'record_id'
    })
    
    role: Optional[str] = None
