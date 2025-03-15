"""
LoginMethod model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import VersionedModel
from enum import Enum


class LoginMethodType(str, Enum):
    EMAIL_PASSWORD = "email-password"


@dataclass
class LoginMethod(VersionedModel):
    """A login method model."""

    person_id: str = None
    method_type: Optional[str] = None
    method_data: Optional[dict] = None
    email_id: Optional[str] = None
    password: Optional[str] = None
