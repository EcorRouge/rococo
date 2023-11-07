"""
Organization model
"""

from dataclasses import dataclass

from . import VersionedModel


@dataclass
class Organization(VersionedModel):
    """An organization model."""

    name: str
