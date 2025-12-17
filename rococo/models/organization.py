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
    # Note: Owner relationships are defined through `PersonOrganizationRole`.
    # Relationships and hierarchy among organizations could be supported in the future.
