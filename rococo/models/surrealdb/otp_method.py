"""
OtpMethod model
"""

from dataclasses import dataclass, field
from typing import List

from . import SurrealVersionedModel


@dataclass
class OtpMethod(SurrealVersionedModel):
    """An OTP method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    secret: str = None
    name: str = None
    enabled: bool = False
    recovery_codes: List[str] = None
