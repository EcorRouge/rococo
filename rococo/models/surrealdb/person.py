"""
Person model
"""

from dataclasses import dataclass

from . import SurrealVersionedModel


@dataclass(repr=False)
class Person(SurrealVersionedModel):
    """A person model."""

    first_name: str = None
    last_name: str = None
