"""
Organization model
"""

from dataclasses import dataclass

from rococo.models import BaseVersionedModel


@dataclass
class BaseOrganization(BaseVersionedModel):
    """An organization model."""

    name: str = None
    code: str = None
    description: str = None
    # members (with accompanied roles, including `owner`)
    # are maintained through `PersonOrganizationRole`
