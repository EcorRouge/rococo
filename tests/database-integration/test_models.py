"""
Test Models for ROC-76 Integration Tests

This module defines the test models used across all database integration tests:
- VersionedProduct: A VersionedModel with Big 6 fields for testing versioning behavior
- NonVersionedConfig: A NonVersionedModel for testing non-versioned entity behavior
- NonVersionedPost: A NonVersionedModel for testing posts (title, description)
- NonVersionedCar: A NonVersionedModel for testing cars (name, brand)
- SurrealVersionedProduct: A SurrealVersionedModel for SurrealDB-specific tests
- SurrealNonVersionedConfig: A SurrealDB-compatible NonVersionedModel for config
- SurrealNonVersionedPost: A SurrealDB-compatible NonVersionedModel for posts
- SurrealNonVersionedCar: A SurrealDB-compatible NonVersionedModel for cars
"""

from dataclasses import dataclass, field
from typing import Optional
from decimal import Decimal

from rococo.models.versioned_model import VersionedModel, get_uuid_hex
from rococo.models import NonVersionedModel


@dataclass(kw_only=True)
class VersionedProduct(VersionedModel):
    """
    A versioned product model for testing.
    
    This model inherits from VersionedModel and includes the Big 6 fields:
    - entity_id
    - version
    - previous_version
    - changed_on
    - changed_by_id
    - active
    
    Additional fields:
    - name: Product name
    - price: Product price (stored as float for database compatibility)
    - description: Optional product description
    """
    name: str = ""
    price: float = 0.0
    description: Optional[str] = None


@dataclass(kw_only=True)
class NonVersionedConfig(NonVersionedModel):
    """
    A non-versioned configuration model for testing.

    This model inherits from NonVersionedModel which only has:
    - entity_id
    - extra (dict for extra fields)

    No Big 6 versioning fields are present.

    Additional fields:
    - key: Configuration key
    - value: Configuration value
    """
    allow_extra = True  # Allow extra fields to be stored as attributes
    key: str = ""
    value: str = ""


@dataclass(kw_only=True)
class NonVersionedPost(NonVersionedModel):
    """
    A non-versioned post model for testing.

    This model inherits from NonVersionedModel which only has:
    - entity_id
    - extra (dict for extra fields)

    No Big 6 versioning fields are present.

    Additional fields:
    - title: Post title
    - description: Post description
    """
    allow_extra = True  # Allow extra fields to be stored as attributes
    title: str = ""
    description: str = ""


@dataclass(kw_only=True)
class NonVersionedCar(NonVersionedModel):
    """
    A non-versioned car model for testing.

    This model inherits from NonVersionedModel which only has:
    - entity_id
    - extra (dict for extra fields)

    No Big 6 versioning fields are present.

    Additional fields:
    - name: Car name/model
    - brand: Car brand/manufacturer
    """
    allow_extra = True  # Allow extra fields to be stored as attributes
    name: str = ""
    brand: str = ""


@dataclass(kw_only=True)
class NonVersionedBrand(NonVersionedModel):
    """
    A non-versioned brand model for testing relationships.

    This model inherits from NonVersionedModel which only has:
    - entity_id
    - extra (dict for extra fields)

    No Big 6 versioning fields are present.

    Additional fields:
    - name: Brand name
    """
    allow_extra = True  # Allow extra fields to be stored as attributes
    name: str = ""


@dataclass(kw_only=True)
class NonVersionedBrandCar(NonVersionedModel):
    """
    A non-versioned junction table model for Brand-Car relationships.

    This model inherits from NonVersionedModel which only has:
    - entity_id
    - extra (dict for extra fields)

    No Big 6 versioning fields are present.

    Additional fields:
    - brand_id: Reference to brand entity_id
    - car_id: Reference to car entity_id
    """
    allow_extra = True  # Allow extra fields to be stored as attributes
    brand_id: str = ""
    car_id: str = ""


# SurrealDB requires a specific model type
try:
    from rococo.models.surrealdb.surreal_versioned_model import SurrealVersionedModel
    
    @dataclass(kw_only=True)
    class SurrealVersionedProduct(SurrealVersionedModel):
        """
        A SurrealDB-specific versioned product model for testing.
        
        This model uses SurrealVersionedModel which has record_id field types
        suitable for SurrealDB's graph-based storage.
        """
        name: str = ""
        price: float = 0.0
        description: Optional[str] = None
    
    @dataclass(kw_only=True)
    class SurrealNonVersionedConfig(NonVersionedModel):
        """
        A SurrealDB-compatible non-versioned configuration model.

        Note: SurrealDB doesn't have a specific NonVersionedModel variant,
        so we use the standard NonVersionedModel.
        """
        key: str = ""
        value: str = ""

    @dataclass(kw_only=True)
    class SurrealNonVersionedPost(NonVersionedModel):
        """
        A SurrealDB-compatible non-versioned post model.

        Note: SurrealDB doesn't have a specific NonVersionedModel variant,
        so we use the standard NonVersionedModel.
        """
        title: str = ""
        description: str = ""

    @dataclass(kw_only=True)
    class SurrealNonVersionedCar(NonVersionedModel):
        """
        A SurrealDB-compatible non-versioned car model.

        Note: SurrealDB doesn't have a specific NonVersionedModel variant,
        so we use the standard NonVersionedModel.
        """
        name: str = ""
        brand: str = ""

    @dataclass(kw_only=True)
    class SurrealNonVersionedBrand(NonVersionedModel):
        """
        A SurrealDB-compatible non-versioned brand model for testing relationships.

        Note: SurrealDB doesn't have a specific NonVersionedModel variant,
        so we use the standard NonVersionedModel.
        """
        name: str = ""

    @dataclass(kw_only=True)
    class SurrealNonVersionedBrandCar(NonVersionedModel):
        """
        A SurrealDB-compatible non-versioned junction table model for Brand-Car relationships.

        Note: SurrealDB doesn't have a specific NonVersionedModel variant,
        so we use the standard NonVersionedModel.
        """
        brand_id: str = ""
        car_id: str = ""

except ImportError:
    # SurrealDB models not available
    SurrealVersionedProduct = None
    SurrealNonVersionedConfig = None
    SurrealNonVersionedPost = None
    SurrealNonVersionedCar = None
    SurrealNonVersionedBrand = None
    SurrealNonVersionedBrandCar = None

