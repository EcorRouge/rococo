"""
PersonOrganizationRole model
"""

from dataclasses import dataclass

from . import VersionedModel


@dataclass
class PersonOrganizationRole(VersionedModel):
    """A person organization role model."""

    person_id: str
    organization_id: str
    role: str
