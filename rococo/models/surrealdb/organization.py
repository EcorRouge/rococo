"""
Organization model
"""

from dataclasses import dataclass

from rococo.models import BaseOrganization
from rococo.models.surrealdb import VersionedModel

@dataclass
class Organization(VersionedModel, BaseOrganization):
    """An organization model."""
    pass # pylint: disable=W0107
