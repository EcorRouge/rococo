"""
OtpMethod model
"""

from dataclasses import dataclass, field
from typing import List

from . import VersionedModel


@dataclass
class OtpMethod(VersionedModel):
    """An OTP method model."""

    person: str = field(metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    secret: str = None
    name: str = None
    enabled: bool = False
    recovery_codes: List[str] = None
