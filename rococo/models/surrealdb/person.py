"""
Person model
"""

from dataclasses import dataclass

from rococo.models import BasePerson
from rococo.models.surrealdb import VersionedModel


@dataclass
class Person(VersionedModel, BasePerson):
    """A person model."""
    pass # pylint: disable=W0107
