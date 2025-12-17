"""
Organization model
"""

from dataclasses import dataclass
from typing import Optional

from . import SurrealVersionedModel


@dataclass
class Organization(SurrealVersionedModel):
    """An organization model."""

    name: Optional[str] = None
    code: Optional[str] = None
    description: Optional[str] = None
    # members (with accompanied roles, including `owner`) are maintained through `PersonOrganizationRole`
    # Note: Owner relationships are defined through `PersonOrganizationRole`.
    # Relationships and hierarchy among organizations could be supported in the future.
