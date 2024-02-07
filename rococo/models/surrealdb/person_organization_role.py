"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field

from rococo.models import BasePersonOrganizationRole
from rococo.models.surrealdb import VersionedModel

# from enum import Enum

# class PersonOrganizationRoleEnum(Enum):
#     OWNER = "OWNER"
#     MANAGER = "MANAGER"
#     MEMBER = "MEMBER"

@dataclass
class PersonOrganizationRole(VersionedModel, BasePersonOrganizationRole):
    """A person organization role model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    organization: str = field(default=None, metadata={
        'relationship': {'model': 'Organization', 'type': 'direct'},
        'field_type': 'record_id'
    })
