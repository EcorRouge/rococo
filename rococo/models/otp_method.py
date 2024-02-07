"""
OtpMethod model
"""

from dataclasses import dataclass
from typing import List

from rococo.models import BaseVersionedModel


@dataclass
class BaseOtpMethod(BaseVersionedModel):
    """An OTP method model."""

    person_id: str = None
    secret: str = None
    name: str = None
    enabled: bool = False
    recovery_codes: List[str] = None
