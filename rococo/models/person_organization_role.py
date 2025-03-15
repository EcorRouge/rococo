"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field

from . import VersionedModel

# from enum import Enum

# class PersonOrganizationRoleEnum(Enum):
#     OWNER = "OWNER"
#     MANAGER = "MANAGER"
#     MEMBER = "MEMBER"

@dataclass
class PersonOrganizationRole(VersionedModel):
    """A person organization role model."""

    person_id: str = None
    organization_id: str = None
    role: str = None
