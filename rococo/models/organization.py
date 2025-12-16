"""
Organization model
"""

from dataclasses import dataclass

from . import VersionedModel
from typing import Optional


@dataclass
class Organization(VersionedModel):
    """An organization model."""

    name: Optional[str] = None
    code: Optional[str] = None
    description: Optional[str] = None
    # members (with accompanied roles, including `owner`) are maintained through `PersonOrganizationRole`

    # Note: consider adding optional redundant owner relationship from Organization to Person
    # despite that roles (including owner) are defined through PersonOrganizationRole

    # we could support relationships and hierarchy among organizations
