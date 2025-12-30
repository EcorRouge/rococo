"""
PersonOrganizationRole model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import VersionedModel


@dataclass
class PersonOrganizationRole(VersionedModel):
    """A person organization role model."""

    person_id: Optional[str] = None
    organization_id: Optional[str] = None
    role: Optional[str] = None
