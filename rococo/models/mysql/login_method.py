"""
LoginMethod model
"""

from dataclasses import dataclass
from typing import Optional

from rococo.models.mysql import VersionedModel


@dataclass
class LoginMethod(VersionedModel):
    """A login method model."""

    person_id: str = None
    method_type: Optional[str] = None
    method_data: Optional[dict] = None
    email_id: str = None
    password: Optional[str] = None
