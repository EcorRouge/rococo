"""
Email model
"""

from dataclasses import dataclass
from typing import Optional

from rococo.models.mysql import VersionedModel

@dataclass
class Email(VersionedModel):
    """A email method model."""

    person_id: str = None
    email: Optional[str] = None
    is_verified: bool = False
    is_default: bool = False
