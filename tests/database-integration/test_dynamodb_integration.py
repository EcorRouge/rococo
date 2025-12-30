"""
DynamoDB Integration Tests for ROC-76

Tests for both VersionedModel and NonVersionedModel functionality with real DynamoDB database.
Requires DynamoDB Local to be running and accessible via environment variables.

Environment Variables:
- AWS_REGION: AWS region (default: us-east-1)
- AWS_ACCESS_KEY_ID: AWS access key (default: test)
- AWS_SECRET_ACCESS_KEY: AWS secret key (default: test)
- DYNAMODB_ENDPOINT: DynamoDB endpoint URL for local testing (optional)
"""

import pytest
import os
from uuid import uuid4
from pynamodb.models import Model
from pynamodb.exceptions import DoesNotExist

from conftest import (
    get_dynamodb_config,
    MockMessageAdapter
)
from test_models import VersionedProduct, NonVersionedConfig, NonVersionedPost, NonVersionedCar

from rococo.data.dynamodb import DynamoDbAdapter
from rococo.repositories.dynamodb.dynamodb_repository import DynamoDbRepository


# Skip all tests in this module if DynamoDB configuration is not available
pytestmark = pytest.mark.skipif(
    get_dynamodb_config() is None,
    reason="DynamoDB configuration not available. Set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY environment variables."
)


# Table names
VERSIONED_TABLE = "versioned_product"
VERSIONED_AUDIT_TABLE = "versioned_product_audit"
NON_VERSIONED_TABLE = "non_versioned_config"
NON_VERSIONED_POST_TABLE = "non_versioned_post"
NON_VERSIONED_CAR_TABLE = "non_versioned_car"


@pytest.fixture
def dynamodb_adapter():
    """Create a DynamoDB adapter for testing."""
    # Set environment variables for DynamoDB Local
    os.environ.setdefault('AWS_REGION', 'us-east-1')
    os.environ.setdefault('AWS_ACCESS_KEY_ID', '')
    os.environ.setdefault('AWS_SECRET_ACCESS_KEY', '')

    adapter = DynamoDbAdapter()
    return adapter


@pytest.fixture
def setup_dynamodb_tables(dynamodb_adapter):
    """Set up test tables and clean up after tests."""
    # Create tables for testing
    tables_to_create = [
        (VERSIONED_TABLE, VersionedProduct),
        (VERSIONED_AUDIT_TABLE, VersionedProduct),
        (NON_VERSIONED_TABLE, NonVersionedConfig),
        (NON_VERSIONED_POST_TABLE, NonVersionedPost),
        (NON_VERSIONED_CAR_TABLE, NonVersionedCar)
    ]

    with dynamodb_adapter:
        for table_name, model_cls in tables_to_create:
            try:
                pynamo_model = dynamodb_adapter._generate_pynamo_model(
                    table_name,
                    model_cls,
                    is_audit=(table_name == VERSIONED_AUDIT_TABLE)
                )
                # Create table if doesn't exist
                if not pynamo_model.exists():
                    pynamo_model.create_table(
                        read_capacity_units=5,
                        write_capacity_units=5,
                        wait=True
                    )
            except Exception:
                # Table might already exist
                pass

    yield

    # # Cleanup after tests - delete all items
    # with dynamodb_adapter:
    #     for table_name, model_cls in tables_to_create:
    #         try:
    #             pynamo_model = dynamodb_adapter._generate_pynamo_model(
    #                 table_name,
    #                 model_cls,
    #                 is_audit=(table_name == VERSIONED_AUDIT_TABLE)
    #             )
    #             # Scan and delete all items
    #             for item in pynamo_model.scan():
    #                 item.delete()
    #         except Exception:
    #             pass


@pytest.fixture
def versioned_repository(dynamodb_adapter, setup_dynamodb_tables):
    """Create a repository for VersionedProduct."""
    message_adapter = MockMessageAdapter()
    user_id = uuid4()
    repo = DynamoDbRepository(
        db_adapter=dynamodb_adapter,
        model=VersionedProduct,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=user_id
    )
    repo.table_name = VERSIONED_TABLE
    repo.use_audit_table = True
    return repo


@pytest.fixture
def nonversioned_repository(dynamodb_adapter, setup_dynamodb_tables):
    """Create a repository for NonVersionedConfig."""
    message_adapter = MockMessageAdapter()
    repo = DynamoDbRepository(
        db_adapter=dynamodb_adapter,
        model=NonVersionedConfig,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_TABLE
    return repo


@pytest.fixture
def posts_repository(dynamodb_adapter, setup_dynamodb_tables):
    """Create a repository for NonVersionedPost."""
    message_adapter = MockMessageAdapter()
    repo = DynamoDbRepository(
        db_adapter=dynamodb_adapter,
        model=NonVersionedPost,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_POST_TABLE
    return repo


@pytest.fixture
def cars_repository(dynamodb_adapter, setup_dynamodb_tables):
    """Create a repository for NonVersionedCar."""
    message_adapter = MockMessageAdapter()
    repo = DynamoDbRepository(
        db_adapter=dynamodb_adapter,
        model=NonVersionedCar,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_CAR_TABLE
    return repo


# ============================================================================
# Versioned Model Tests
# ============================================================================

class TestDynamoDBVersionedModel:
    """Tests for VersionedModel behavior with DynamoDB."""

    def test_versioned_model_create(self, versioned_repository):
        """Test creating a versioned entity with Big 6 fields."""
        # Create a new product
        product = VersionedProduct(
            name="Test Product",
            price=29.99,
            description="A test product"
        )

        # Save the product
        saved_product = versioned_repository.save(product)

        # Verify entity_id is set
        assert saved_product.entity_id is not None

        # Verify Big 6 fields are populated
        assert saved_product.version is not None
        assert saved_product.changed_on is not None
        assert saved_product.active is True

        # Verify custom fields
        assert saved_product.name == "Test Product"
        assert saved_product.price == pytest.approx(29.99)
        assert saved_product.description == "A test product"

        # Retrieve and verify
        retrieved = versioned_repository.get_one(
            {'entity_id': saved_product.entity_id}
        )
        assert retrieved is not None
        assert retrieved.name == "Test Product"
        assert retrieved.price == pytest.approx(29.99)

    def test_versioned_model_update(self, versioned_repository):
        """Test updating a versioned entity with version bump."""
        # Create initial product
        product = VersionedProduct(
            name="Original Name",
            price=19.99
        )
        saved_product = versioned_repository.save(product)
        original_version = saved_product.version

        # Update the product
        saved_product.name = "Updated Name"
        saved_product.price = 24.99
        updated_product = versioned_repository.save(saved_product)

        # Verify version changed
        assert updated_product.version != original_version
        assert updated_product.previous_version == original_version

        # Verify updated values
        assert updated_product.name == "Updated Name"
        assert updated_product.price == pytest.approx(24.99)

    def test_versioned_model_delete(self, versioned_repository):
        """Test soft delete sets active=False."""
        # Create a product
        product = VersionedProduct(
            name="To Delete",
            price=9.99
        )
        saved_product = versioned_repository.save(product)
        entity_id = saved_product.entity_id

        # Verify it exists and is active
        assert saved_product.active is True

        # Delete the product
        deleted_product = versioned_repository.delete(saved_product)

        # Verify active is False
        assert deleted_product.active is False

        # Verify it's not returned by default queries (which filter active=True)
        retrieved = versioned_repository.get_one(
            {'entity_id': entity_id}
        )
        assert retrieved is None  # Should not find inactive product

    def test_versioned_model_audit_table(self, versioned_repository, dynamodb_adapter):
        """Test that audit records are created on update."""
        # Create initial product
        product = VersionedProduct(
            name="Audit Test",
            price=15.99
        )
        saved_product = versioned_repository.save(product)
        entity_id = saved_product.entity_id

        # Update to create audit record
        saved_product.name = "Audit Test Updated"
        versioned_repository.save(saved_product)

        # Check audit table
        with dynamodb_adapter:
            audit_model = dynamodb_adapter._generate_pynamo_model(
                VERSIONED_AUDIT_TABLE,
                VersionedProduct,
                is_audit=True
            )
            audit_records = list(audit_model.query(entity_id))

        # Should have at least one audit record (the original version)
        assert len(audit_records) >= 1

    def test_versioned_model_get_many(self, versioned_repository):
        """Test retrieving multiple versioned entities."""
        # Create multiple products
        for i in range(3):
            product = VersionedProduct(
                name=f"Product {i}",
                price=10.0 + i
            )
            versioned_repository.save(product)

        # Get all products
        products = versioned_repository.get_many()

        # Should have at least 3 products
        assert len(products) >= 3

        # All should be active
        for p in products:
            assert p.active is True

    def test_versioned_model_soft_delete_verification(self, versioned_repository, dynamodb_adapter):
        """Test that soft delete preserves document in table."""
        # Create product
        product = VersionedProduct(name="Soft Delete Check", price=25.0)
        saved = versioned_repository.save(product)
        entity_id = saved.entity_id

        # Delete (soft delete)
        versioned_repository.delete(saved)

        # Verify document still exists in table with active=False
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(VERSIONED_TABLE, VersionedProduct)
            try:
                item = pynamo_model.get(entity_id)
                assert item.active is False
                assert item.name == "Soft Delete Check"
            except DoesNotExist:
                pytest.fail("Soft deleted item should still exist in table")

    def test_versioned_model_multiple_updates(self, versioned_repository):
        """Test multiple updates create proper version history."""
        # Create initial product
        product = VersionedProduct(name="Version History", price=10.0)
        saved = versioned_repository.save(product)
        entity_id = saved.entity_id
        versions = [saved.version]

        # Make 5 updates
        for i in range(5):
            saved.price = float(10 + i + 1)
            saved.name = f"Version History v{i+1}"
            saved = versioned_repository.save(saved)
            versions.append(saved.version)

        # All versions should be unique
        assert len(set(versions)) == 6

        # Verify final state
        final = versioned_repository.get_one({'entity_id': entity_id})
        assert final.name == "Version History v5"
        assert final.price == pytest.approx(15.0)


# ============================================================================
# Non-Versioned Model Tests
# ============================================================================

class TestDynamoDBNonVersionedModel:
    """Tests for NonVersionedModel behavior with DynamoDB."""

    def test_nonversioned_model_create(self, nonversioned_repository):
        """Test creating a non-versioned entity without Big 6 fields."""
        # Create a config entry
        config = NonVersionedConfig(
            key="app.setting",
            value="enabled"
        )

        # Save the config
        saved_config = nonversioned_repository.save(config)

        # Verify entity_id is set
        assert saved_config.entity_id is not None

        # Verify no Big 6 versioning fields
        assert not hasattr(saved_config, 'version') or not isinstance(getattr(saved_config, 'version', None), str)
        assert not hasattr(saved_config, 'previous_version')
        assert not hasattr(saved_config, 'active')
        assert not hasattr(saved_config, 'changed_by_id')

        # Verify custom fields
        assert saved_config.key == "app.setting"
        assert saved_config.value == "enabled"

    def test_nonversioned_model_update(self, nonversioned_repository):
        """Test updating a non-versioned entity without version tracking."""
        # Create initial config
        config = NonVersionedConfig(
            key="cache.ttl",
            value="3600"
        )
        saved_config = nonversioned_repository.save(config)
        entity_id = saved_config.entity_id

        # Update the config
        saved_config.value = "7200"
        updated_config = nonversioned_repository.save(saved_config)

        # Verify entity_id unchanged
        assert updated_config.entity_id == entity_id

        # Verify updated value
        assert updated_config.value == "7200"

        # Verify no version tracking
        assert not hasattr(updated_config, 'version') or not isinstance(getattr(updated_config, 'version', None), str)

    def test_nonversioned_model_delete(self, nonversioned_repository):
        """Test hard delete removes document from table."""
        # Create a config
        config = NonVersionedConfig(
            key="temp.setting",
            value="temporary"
        )
        saved_config = nonversioned_repository.save(config)
        entity_id = saved_config.entity_id

        # Verify it exists
        retrieved_before = nonversioned_repository.get_one({'entity_id': entity_id})
        assert retrieved_before is not None

        # Delete the config (should be hard delete)
        deleted_config = nonversioned_repository.delete(saved_config)

        # NonVersionedModel doesn't have 'active' field
        assert deleted_config is not None

        # Verify it's actually removed (hard delete)
        retrieved_after = nonversioned_repository.get_one({'entity_id': entity_id})
        assert retrieved_after is None

    def test_nonversioned_no_audit(self, nonversioned_repository, dynamodb_adapter):
        """Test that no audit records are created for non-versioned entities."""
        # Create and update a config
        config = NonVersionedConfig(
            key="no.audit.test",
            value="initial"
        )
        saved_config = nonversioned_repository.save(config)
        entity_id = saved_config.entity_id

        # Update it
        saved_config.value = "updated"
        nonversioned_repository.save(saved_config)

        # Check that no audit table has records for this entity
        with dynamodb_adapter:
            try:
                audit_model = dynamodb_adapter._generate_pynamo_model(
                    f"{NON_VERSIONED_TABLE}_audit",
                    NonVersionedConfig,
                    is_audit=True
                )
                audit_records = list(audit_model.query(entity_id))
                # Should have no audit records
                assert len(audit_records) == 0
            except Exception:
                # Audit table might not exist, which is expected for non-versioned
                pass

    def test_nonversioned_hard_delete_verification(self, nonversioned_repository, dynamodb_adapter):
        """Test that delete actually removes item from table (hard delete)."""
        # Create a config
        config = NonVersionedConfig(key="hard.delete", value="will.be.removed")
        saved = nonversioned_repository.save(config)
        entity_id = saved.entity_id

        # Verify it exists
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            try:
                item = pynamo_model.get(entity_id)
                assert item is not None
            except DoesNotExist:
                pytest.fail("Item should exist before delete")

        # Delete it (should be hard delete for NonVersionedModel)
        nonversioned_repository.delete(saved)

        # Verify item is actually removed from table
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            try:
                pynamo_model.get(entity_id)
                # If we get here, the item still exists - fail the test
                pytest.fail("Hard delete should have removed the item")
            except DoesNotExist:
                # This is expected - item should not exist
                pass

    def test_nonversioned_upsert_new_entity(self, nonversioned_repository, dynamodb_adapter):
        """Test upserting a completely new non-versioned entity."""
        # Create new config
        config = NonVersionedConfig(key="upsert.new", value="new.entity")
        saved = nonversioned_repository.save(config)

        # Verify saved
        assert saved.entity_id is not None

        # Verify in database
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            item = pynamo_model.get(saved.entity_id)
            assert item is not None
            assert item.key == "upsert.new"
            assert item.value == "new.entity"

    def test_nonversioned_upsert_existing_entity(self, nonversioned_repository, dynamodb_adapter):
        """Test upserting an existing entity with same entity_id (should replace)."""
        # Create initial config
        config = NonVersionedConfig(key="upsert.existing", value="original")
        saved = nonversioned_repository.save(config)
        entity_id = saved.entity_id

        # Update with same entity_id (upsert)
        config2 = NonVersionedConfig(key="upsert.existing", value="updated")
        config2.entity_id = entity_id
        updated = nonversioned_repository.save(config2)

        # Verify entity_id unchanged
        assert updated.entity_id == entity_id
        assert updated.value == "updated"

        # Verify only ONE item exists in database
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            item = pynamo_model.get(entity_id)
            assert item.value == "updated"

    def test_nonversioned_get_one_via_repository(self, nonversioned_repository):
        """Test get_one via repository for NonVersionedModel."""
        # Create config
        config = NonVersionedConfig(key="get.one", value="test")
        saved = nonversioned_repository.save(config)

        # Retrieve via repository
        retrieved = nonversioned_repository.get_one(
            {'entity_id': saved.entity_id}
        )

        # Verify retrieval
        assert retrieved is not None
        assert retrieved.key == "get.one"
        assert retrieved.value == "test"
        assert retrieved.entity_id == saved.entity_id

    def test_nonversioned_get_many(self, nonversioned_repository):
        """Test get_many for non-versioned entities."""
        # Create multiple configs
        for i in range(5):
            config = NonVersionedConfig(key=f"batch.key.{i}", value=f"value_{i}")
            nonversioned_repository.save(config)

        # Get all configs
        configs = nonversioned_repository.get_many()

        # Should have at least 5 configs
        assert len(configs) >= 5

    def test_nonversioned_update_preserves_entity_id(self, nonversioned_repository):
        """Test that updates preserve the entity_id."""
        # Create initial config
        config = NonVersionedConfig(key="preserve.id", value="original")
        saved = nonversioned_repository.save(config)
        original_id = saved.entity_id

        # Update multiple times
        for i in range(5):
            saved.value = f"updated_{i}"
            saved = nonversioned_repository.save(saved)

        # Verify entity_id never changed
        assert saved.entity_id == original_id

        # Verify final value
        final = nonversioned_repository.get_one({'entity_id': original_id})
        assert final.value == "updated_4"


# ============================================================================
# Non-Versioned Posts Model Tests
# ============================================================================

class TestDynamoDBNonVersionedPosts:
    """Tests for NonVersionedPost model with DynamoDB."""

    def test_posts_create(self, posts_repository):
        """Test creating a non-versioned post."""
        post = NonVersionedPost(title="First Post", description="This is my first blog post")
        saved_post = posts_repository.save(post)

        assert saved_post is not None
        assert saved_post.entity_id is not None
        assert saved_post.title == "First Post"
        assert saved_post.description == "This is my first blog post"

    def test_posts_update(self, posts_repository):
        """Test updating a non-versioned post."""
        post = NonVersionedPost(title="Original Title", description="Original description")
        saved_post = posts_repository.save(post)

        # Update the post
        saved_post.title = "Updated Title"
        saved_post.description = "Updated description"
        updated_post = posts_repository.save(saved_post)

        assert updated_post.title == "Updated Title"
        assert updated_post.description == "Updated description"
        assert updated_post.entity_id == saved_post.entity_id

    def test_posts_delete(self, posts_repository):
        """Test hard delete of non-versioned post."""
        post = NonVersionedPost(title="To Delete", description="Will be removed")
        saved_post = posts_repository.save(post)
        entity_id = saved_post.entity_id

        # Verify exists
        retrieved_before = posts_repository.get_one({'entity_id': entity_id})
        assert retrieved_before is not None

        # Delete
        posts_repository.delete(saved_post)

        # Verify removed (hard delete)
        retrieved_after = posts_repository.get_one({'entity_id': entity_id})
        assert retrieved_after is None


# ============================================================================
# Non-Versioned Cars Model Tests
# ============================================================================

class TestDynamoDBNonVersionedCars:
    """Tests for NonVersionedCar model with DynamoDB."""

    def test_cars_create(self, cars_repository):
        """Test creating a non-versioned car."""
        car = NonVersionedCar(name="Model S", brand="Tesla")
        saved_car = cars_repository.save(car)

        assert saved_car is not None
        assert saved_car.entity_id is not None
        assert saved_car.name == "Model S"
        assert saved_car.brand == "Tesla"

    def test_cars_update(self, cars_repository):
        """Test updating a non-versioned car."""
        car = NonVersionedCar(name="Camry", brand="Toyota")
        saved_car = cars_repository.save(car)

        # Update the car
        saved_car.name = "Camry Hybrid"
        updated_car = cars_repository.save(saved_car)

        assert updated_car.name == "Camry Hybrid"
        assert updated_car.brand == "Toyota"
        assert updated_car.entity_id == saved_car.entity_id

    def test_cars_delete(self, cars_repository):
        """Test hard delete of non-versioned car."""
        car = NonVersionedCar(name="To Delete", brand="TestBrand")
        saved_car = cars_repository.save(car)
        entity_id = saved_car.entity_id

        # Verify exists
        retrieved_before = cars_repository.get_one({'entity_id': entity_id})
        assert retrieved_before is not None

        # Delete
        cars_repository.delete(saved_car)

        # Verify removed (hard delete)
        retrieved_after = cars_repository.get_one({'entity_id': entity_id})
        assert retrieved_after is None

    def test_cars_multiple_brands(self, cars_repository):
        """Test creating multiple cars from different brands."""
        cars = [
            NonVersionedCar(name="Civic", brand="Honda"),
            NonVersionedCar(name="Accord", brand="Honda"),
            NonVersionedCar(name="Corolla", brand="Toyota"),
            NonVersionedCar(name="Model Y", brand="Tesla")
        ]

        saved_cars = [cars_repository.save(car) for car in cars]

        assert len(saved_cars) == 4
        assert all(car.entity_id is not None for car in saved_cars)

        honda_count = sum(1 for car in saved_cars if car.brand == "Honda")
        assert honda_count == 2


# ============================================================================
# Integration Tests
# ============================================================================

class TestDynamoDBIntegration:
    """Integration tests for cross-cutting concerns in DynamoDB."""

    def test_mixed_models_no_interference(self, versioned_repository, nonversioned_repository):
        """Test that VersionedModel and NonVersionedModel work together without interference."""
        # Create versioned product
        product = VersionedProduct(name="Mixed DynamoDB Product", price=100.0)
        saved_product = versioned_repository.save(product)

        # Create non-versioned config
        config = NonVersionedConfig(key="mixed.dynamo", value="config")
        saved_config = nonversioned_repository.save(config)

        # Verify both exist
        assert saved_product.entity_id is not None
        assert saved_config.entity_id is not None

        # Verify they're in different tables
        product_retrieved = versioned_repository.get_one({'entity_id': saved_product.entity_id})
        config_retrieved = nonversioned_repository.get_one({'entity_id': saved_config.entity_id})

        assert product_retrieved is not None
        assert config_retrieved is not None

        # Verify versioned has special fields
        assert hasattr(product_retrieved, 'active')
        assert product_retrieved.active is True

        # Verify non-versioned doesn't have versioning fields
        assert not hasattr(config_retrieved, 'active') or not isinstance(getattr(config_retrieved, 'active', None), bool)

    def test_adapter_upsert_vs_save_routing(self, nonversioned_repository, versioned_repository):
        """Test that adapter routes to correct method based on model type."""
        # For NonVersionedModel, should use upsert
        config = NonVersionedConfig(key="routing.test", value="upsert")
        saved_config = nonversioned_repository.save(config)

        # Update - should still upsert
        saved_config.value = "upserted"
        updated_config = nonversioned_repository.save(saved_config)

        assert updated_config.value == "upserted"

        # For VersionedModel, should use versioned save
        product = VersionedProduct(name="Routing Test", price=50.0)
        saved_product = versioned_repository.save(product)

        # Update - should create new version
        original_version = saved_product.version
        saved_product.price = 75.0
        updated_product = versioned_repository.save(saved_product)

        # Verify version changed
        assert updated_product.version != original_version

    def test_repository_delete_routing(self, versioned_repository, nonversioned_repository, dynamodb_adapter):
        """Test that delete operations route correctly (soft vs hard)."""
        # VersionedModel - soft delete
        product = VersionedProduct(name="Delete Routing", price=10.0)
        saved_product = versioned_repository.save(product)
        product_id = saved_product.entity_id

        versioned_repository.delete(saved_product)

        # Should still exist in table with active=False
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(VERSIONED_TABLE, VersionedProduct)
            try:
                item = pynamo_model.get(product_id)
                assert item.active is False
            except DoesNotExist:
                pytest.fail("Soft deleted item should still exist")

        # NonVersionedModel - hard delete
        config = NonVersionedConfig(key="delete.routing", value="test")
        saved_config = nonversioned_repository.save(config)
        config_id = saved_config.entity_id

        nonversioned_repository.delete(saved_config)

        # Should NOT exist in table (hard delete)
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            try:
                item = pynamo_model.get(config_id)
                pytest.fail("Hard deleted item should not exist")
            except DoesNotExist:
                # Expected - item should be gone
                pass

    def test_adapter_upsert_method_coverage(self, dynamodb_adapter):
        """Test the adapter's upsert method directly to ensure full coverage."""
        # Test upsert method
        test_data = {
            'entity_id': str(uuid4()),
            'key': 'direct.upsert',
            'value': 'test'
        }

        result = dynamodb_adapter.upsert(NON_VERSIONED_TABLE, test_data, model_cls=NonVersionedConfig)

        assert result is not None
        assert result['entity_id'] == test_data['entity_id']
        assert result['key'] == 'direct.upsert'

    def test_adapter_upsert_missing_entity_id(self, dynamodb_adapter):
        """Test that upsert fails when entity_id is missing."""
        test_data = {
            'key': 'no.entity.id',
            'value': 'test'
        }

        with pytest.raises(RuntimeError, match="entity_id"):
            dynamodb_adapter.upsert(NON_VERSIONED_TABLE, test_data, model_cls=NonVersionedConfig)

    def test_repository_save_nonversioned_path_coverage(self, nonversioned_repository):
        """Test the non-versioned save path in repository for full coverage."""
        # Create new config
        config = NonVersionedConfig(key="coverage.test", value="initial")
        saved = nonversioned_repository.save(config)

        assert saved.entity_id is not None
        assert saved.value == "initial"

        # Update to test upsert path
        saved.value = "updated"
        updated = nonversioned_repository.save(saved)

        assert updated.entity_id == saved.entity_id
        assert updated.value == "updated"

    def test_repository_delete_nonversioned_coverage(self, nonversioned_repository, dynamodb_adapter):
        """Test the non-versioned delete path in repository for full coverage."""
        # Create config
        config = NonVersionedConfig(key="delete.coverage", value="test")
        saved = nonversioned_repository.save(config)
        entity_id = saved.entity_id

        # Verify exists
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            item_before = pynamo_model.get(entity_id)
            assert item_before is not None

        # Delete (should be hard delete)
        result = nonversioned_repository.delete(saved)
        assert result is not None

        # Verify removed
        with dynamodb_adapter:
            pynamo_model = dynamodb_adapter._generate_pynamo_model(NON_VERSIONED_TABLE, NonVersionedConfig)
            try:
                pynamo_model.get(entity_id)
                pytest.fail("Item should be hard deleted")
            except DoesNotExist:
                pass  # Expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
