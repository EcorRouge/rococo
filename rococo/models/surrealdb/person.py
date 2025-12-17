"""
Person model
"""

from dataclasses import dataclass
from typing import Optional

from . import SurrealVersionedModel


@dataclass(repr=False)
class Person(SurrealVersionedModel):
    """A person model."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
