"""
Organization model
"""

from dataclasses import dataclass

from . import VersionedModel
from typing import Optional


@dataclass
class Organization(VersionedModel):
    """An organization model."""

    name: str = None
    code: Optional[str] = None
    description: Optional[str] = None
    # members (with accompanied roles, including `owner`) are maintained through `PersonOrganizationRole`

    # TODO: to see do we want redundancy, to have a relationship from an `Organization` to its `owner`
    # despite that `roles` in Organization (including the `owner`) are defined through `PersonOrganizationRole`

    # we could support relationships and hierarchy among organizations
