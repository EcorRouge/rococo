"""
Email model
"""

from dataclasses import dataclass

from . import VersionedModel


@dataclass
class Email(VersionedModel):
    """A email method model."""

    person_id: str
    email: str
    is_verified: bool = False
    is_default: bool = False
