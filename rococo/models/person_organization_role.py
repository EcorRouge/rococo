"""
PersonOrganizationRole model
"""

from dataclasses import dataclass

from rococo.models import BaseVersionedModel

# from enum import Enum

# class PersonOrganizationRoleEnum(Enum):
#     OWNER = "OWNER"
#     MANAGER = "MANAGER"
#     MEMBER = "MEMBER"

@dataclass
class BasePersonOrganizationRole(BaseVersionedModel):
    """A person organization role model."""

    person_id: str = None
    organization_id: str = None

    # TODO: We would benefit from strictly typed Enum for role, but flexibility would lower
    # role: PersonOrganizationRoleEnum = PersonOrganizationRoleEnum.MEMBER
    role: str = None
