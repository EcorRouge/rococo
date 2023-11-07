"""
LoginMethod model
"""

from dataclasses import dataclass
from typing import Optional

from . import VersionedModel


@dataclass
class LoginMethod(VersionedModel):
    """A login method model."""

    person_id: str
    method_type: str
    method_data: Optional[dict]
    email: Optional[str]
    password: Optional[str]
