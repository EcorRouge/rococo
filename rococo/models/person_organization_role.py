"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field

from . import VersionedModel

@dataclass
class PersonOrganizationRole(VersionedModel):
    """A person organization role model."""

    person: str = field(metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    organization_id: str
    role: str
