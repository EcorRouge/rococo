"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field

from . import SurrealVersionedModel

# from enum import Enum

# class PersonOrganizationRoleEnum(Enum):
#     OWNER = "OWNER"
#     MANAGER = "MANAGER"
#     MEMBER = "MEMBER"

@dataclass
class PersonOrganizationRole(SurrealVersionedModel):
    """A person organization role model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    organization: str = field(default=None, metadata={
        'relationship': {'model': 'Organization', 'type': 'direct'},
        'field_type': 'record_id'
    })
    
    # TODO: We would benefit from strictly typed Enum for role, but flexibility would lower
    # role: PersonOrganizationRoleEnum = PersonOrganizationRoleEnum.MEMBER
    role: str = None
