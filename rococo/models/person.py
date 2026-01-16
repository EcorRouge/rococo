"""
Person model
"""

from dataclasses import dataclass
from typing import Optional

from . import VersionedModel


@dataclass(repr=False)
class Person(VersionedModel):
    """A person model."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
