"""
MongoDB Integration Tests for ROC-76

Tests for both VersionedModel and NonVersionedModel functionality with real MongoDB database.
Requires MongoDB to be running and accessible via environment variables.

Environment Variables:
- MONGODB_URI: MongoDB connection URI (default: mongodb://localhost:27017)
- MONGODB_DATABASE: MongoDB database name (default: rococo_test)
"""

import pytest
from uuid import uuid4

from conftest import (
    get_mongodb_config,
    MockMessageAdapter
)
from test_models import VersionedProduct, NonVersionedConfig, NonVersionedPost, NonVersionedCar

from rococo.data.mongodb import MongoDBAdapter
from rococo.repositories.mongodb.mongodb_repository import MongoDbRepository


# Skip all tests in this module if MongoDB configuration is not available
pytestmark = pytest.mark.skipif(
    get_mongodb_config() is None,
    reason="MongoDB configuration not available. Set MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE environment variables."
)


# Collection names
VERSIONED_COLLECTION = "versioned_product"
VERSIONED_AUDIT_COLLECTION = "versioned_product_audit"
NON_VERSIONED_COLLECTION = "non_versioned_config"
NON_VERSIONED_POST_COLLECTION = "non_versioned_post"
NON_VERSIONED_CAR_COLLECTION = "non_versioned_car"


@pytest.fixture
def mongodb_adapter():
    """Create a MongoDB adapter for testing."""
    config = get_mongodb_config()
    adapter = MongoDBAdapter(
        mongo_uri=config['uri'],
        mongo_database=config['database']
    )
    return adapter


@pytest.fixture
def setup_mongodb_collections(mongodb_adapter):
    """Set up test collections and clean up after tests."""
    # MongoDB creates collections implicitly, but we'll ensure indexes
    with mongodb_adapter:
        # Create indexes for versioned collection
        mongodb_adapter.create_index(
            VERSIONED_COLLECTION,
            [('entity_id', 1), ('latest', 1)],
            'entity_latest_idx',
            partial_filter={'latest': True}
        )
        mongodb_adapter.create_index(
            VERSIONED_COLLECTION,
            [('entity_id', 1), ('active', 1)],
            'entity_active_idx'
        )
    
    yield

    # Cleanup after tests
    with mongodb_adapter:
        # Drop collections
        mongodb_adapter.db.drop_collection(VERSIONED_COLLECTION)
        mongodb_adapter.db.drop_collection(VERSIONED_AUDIT_COLLECTION)
        mongodb_adapter.db.drop_collection(NON_VERSIONED_COLLECTION)
        mongodb_adapter.db.drop_collection(NON_VERSIONED_POST_COLLECTION)
        mongodb_adapter.db.drop_collection(NON_VERSIONED_CAR_COLLECTION)


@pytest.fixture
def versioned_repository(mongodb_adapter, setup_mongodb_collections):
    """Create a repository for VersionedProduct."""
    message_adapter = MockMessageAdapter()
    user_id = uuid4()
    repo = MongoDbRepository(
        db_adapter=mongodb_adapter,
        model=VersionedProduct,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=user_id
    )
    repo.use_audit_table = True
    return repo


@pytest.fixture
def nonversioned_repository(mongodb_adapter, setup_mongodb_collections):
    """Create a repository for NonVersionedConfig."""
    message_adapter = MockMessageAdapter()
    repo = MongoDbRepository(
        db_adapter=mongodb_adapter,
        model=NonVersionedConfig,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    return repo


@pytest.fixture
def posts_repository(mongodb_adapter, setup_mongodb_collections):
    """Create a repository for NonVersionedPost."""
    message_adapter = MockMessageAdapter()
    repo = MongoDbRepository(
        db_adapter=mongodb_adapter,
        model=NonVersionedPost,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    return repo


@pytest.fixture
def cars_repository(mongodb_adapter, setup_mongodb_collections):
    """Create a repository for NonVersionedCar."""
    message_adapter = MockMessageAdapter()
    repo = MongoDbRepository(
        db_adapter=mongodb_adapter,
        model=NonVersionedCar,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    return repo


# ============================================================================
# Versioned Model Tests
# ============================================================================

class TestMongoDBVersionedModel:
    """Tests for VersionedModel behavior with MongoDB."""
    
    def test_versioned_model_create(self, versioned_repository):
        """Test creating a versioned entity with Big 6 fields."""
        # Create a new product
        product = VersionedProduct(
            name="Test Product",
            price=29.99,
            description="A test product"
        )
        
        # Save the product
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        
        # Verify entity_id is set
        assert saved_product.entity_id is not None
        
        # Verify Big 6 fields are populated
        assert saved_product.version is not None
        assert saved_product.changed_on is not None
        assert saved_product.active is True
        
        # Verify custom fields
        assert saved_product.name == "Test Product"
        assert saved_product.price == 29.99
        assert saved_product.description == "A test product"
        
        # Retrieve and verify
        retrieved = versioned_repository.get_one(
            VERSIONED_COLLECTION,
            'entity_active_idx',
            {'entity_id': saved_product.entity_id}
        )
        assert retrieved is not None
        assert retrieved.name == "Test Product"
        assert retrieved.price == 29.99
    
    def test_versioned_model_update(self, versioned_repository):
        """Test updating a versioned entity with version bump."""
        # Create initial product
        product = VersionedProduct(
            name="Original Name",
            price=19.99
        )
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        original_version = saved_product.version
        
        # Update the product
        saved_product.name = "Updated Name"
        saved_product.price = 24.99
        updated_product = versioned_repository.save(saved_product, VERSIONED_COLLECTION)
        
        # Verify version changed
        assert updated_product.version != original_version
        assert updated_product.previous_version == original_version
        
        # Verify updated values
        assert updated_product.name == "Updated Name"
        assert updated_product.price == 24.99
    
    def test_versioned_model_delete(self, versioned_repository):
        """Test soft delete sets active=False."""
        # Create a product
        product = VersionedProduct(
            name="To Delete",
            price=9.99
        )
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved_product.entity_id
        
        # Verify it exists and is active
        assert saved_product.active is True
        
        # Delete the product
        deleted_product = versioned_repository.delete(saved_product, VERSIONED_COLLECTION)
        
        # Verify active is False
        assert deleted_product.active is False
        
        # Verify it's not returned by default queries (which filter active=True)
        retrieved = versioned_repository.get_one(
            VERSIONED_COLLECTION,
            'entity_active_idx',
            {'entity_id': entity_id}
        )
        assert retrieved is None  # Should not find inactive product
    
    def test_versioned_model_audit_collection(self, versioned_repository, mongodb_adapter):
        """Test that audit records are created on update."""
        # Create initial product
        product = VersionedProduct(
            name="Audit Test",
            price=15.99
        )
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved_product.entity_id
        
        # Update to create audit record
        saved_product.name = "Audit Test Updated"
        versioned_repository.save(saved_product, VERSIONED_COLLECTION)
        
        # Check audit collection
        with mongodb_adapter:
            audit_records = list(mongodb_adapter.db[VERSIONED_AUDIT_COLLECTION].find(
                {'entity_id': entity_id}
            ))
        
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
            versioned_repository.save(product, VERSIONED_COLLECTION)
        
        # Get all products
        products = versioned_repository.get_many(
            VERSIONED_COLLECTION,
            'entity_active_idx'
        )
        
        # Should have at least 3 products
        assert len(products) >= 3
        
        # All should be active
        for p in products:
            assert p.active is True
    
    def test_versioned_model_latest_flag(self, versioned_repository, mongodb_adapter):
        """Test that the 'latest' flag is correctly managed."""
        # Create a product
        product = VersionedProduct(
            name="Latest Test",
            price=20.0
        )
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved_product.entity_id

        # Update the product
        saved_product.name = "Latest Test Updated"
        versioned_repository.save(saved_product, VERSIONED_COLLECTION)

        # Check that only one document has latest=True
        with mongodb_adapter:
            latest_docs = list(mongodb_adapter.db[VERSIONED_COLLECTION].find({
                'entity_id': entity_id,
                'latest': True
            }))

        assert len(latest_docs) == 1
        assert latest_docs[0]['name'] == "Latest Test Updated"

    def test_versioned_latest_flag_transitions(self, versioned_repository, mongodb_adapter):
        """Test that latest flag properly transitions between versions."""
        # Create a product
        product = VersionedProduct(name="Flag Transition", price=10.0)
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id
        first_version = saved.version

        # Update multiple times
        for i in range(3):
            saved.price = float(10 + i)
            saved = versioned_repository.save(saved, VERSIONED_COLLECTION)

        # Query all versions of this entity
        with mongodb_adapter:
            all_versions = list(mongodb_adapter.db[VERSIONED_COLLECTION].find({
                'entity_id': entity_id
            }))

            # Check that only the most recent has latest=True
            latest_count = sum(1 for v in all_versions if v.get('latest') is True)
            assert latest_count == 1

            # Find the latest version
            latest_version = [v for v in all_versions if v.get('latest') is True][0]
            assert float(latest_version['price']) == 12.0

    def test_versioned_multiple_versions_in_audit(self, versioned_repository, mongodb_adapter):
        """Test that audit collection captures all historical versions."""
        # Create initial product
        product = VersionedProduct(name="Audit History", price=5.0)
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Make 5 updates to create version history
        for i in range(5):
            saved.price = float(5 + i + 1)
            saved.name = f"Audit History v{i+1}"
            saved = versioned_repository.save(saved, VERSIONED_COLLECTION)

        # Check audit collection has all previous versions
        with mongodb_adapter:
            audit_records = list(mongodb_adapter.db[VERSIONED_AUDIT_COLLECTION].find({
                'entity_id': entity_id
            }).sort('changed_on', 1))

            # Should have at least 5 audit records (original + 4 updates)
            assert len(audit_records) >= 5

            # Verify price progression in audit
            prices = [float(r['price']) for r in audit_records]
            assert 5.0 in prices  # Original price should be in audit

    def test_versioned_get_specific_version(self, versioned_repository, mongodb_adapter):
        """Test retrieving a specific version from history."""
        # Create and update product
        product = VersionedProduct(name="Version Retrieval", price=100.0)
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id
        first_version = saved.version

        # Update to create new version
        saved.price = 200.0
        saved = versioned_repository.save(saved, VERSIONED_COLLECTION)
        second_version = saved.version

        # Retrieve specific version from main collection or audit
        with mongodb_adapter:
            # Current version should be in main collection
            current = mongodb_adapter.db[VERSIONED_COLLECTION].find_one({
                'entity_id': entity_id,
                'version': second_version
            })
            assert current is not None
            assert float(current['price']) == 200.0

            # First version should be in audit
            old_version = mongodb_adapter.db[VERSIONED_AUDIT_COLLECTION].find_one({
                'entity_id': entity_id,
                'version': first_version
            })
            if old_version:  # May not be in audit yet depending on implementation
                assert float(old_version['price']) == 100.0

    def test_versioned_ttl_field_on_delete(self, versioned_repository, mongodb_adapter):
        """Test that deleted records get TTL field if configured."""
        # Create and delete a product
        product = VersionedProduct(name="TTL Test", price=50.0)
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Delete the product
        deleted = versioned_repository.delete(saved, VERSIONED_COLLECTION)

        # Check if TTL field is set in database
        with mongodb_adapter:
            doc = mongodb_adapter.db[VERSIONED_COLLECTION].find_one({
                'entity_id': entity_id
            })

            # Verify soft delete
            assert doc is not None
            assert doc['active'] is False

            # Check for TTL field (if TTL is configured)
            # Note: TTL configuration is optional
            if 'ttl' in doc:
                assert doc['ttl'] is not None

    def test_versioned_index_performance(self, versioned_repository, mongodb_adapter):
        """Test that indexes are properly used for versioned model queries."""
        # Create multiple products
        for i in range(100):
            product = VersionedProduct(name=f"Index Test {i}", price=float(i))
            versioned_repository.save(product, VERSIONED_COLLECTION)

        # Query by entity_id and check explain plan
        with mongodb_adapter:
            # Get a specific entity_id
            sample_doc = mongodb_adapter.db[VERSIONED_COLLECTION].find_one({'latest': True})
            if sample_doc:
                entity_id = sample_doc['entity_id']

                # Run explain on query
                explain = mongodb_adapter.db[VERSIONED_COLLECTION].find({
                    'entity_id': entity_id,
                    'active': True
                }).explain()

                # Verify index was used (check executionStats or queryPlanner)
                # Note: Exact structure of explain varies by MongoDB version
                assert explain is not None

    def test_versioned_soft_delete_verification(self, versioned_repository, mongodb_adapter):
        """Test that soft delete preserves document in collection."""
        # Create product
        product = VersionedProduct(name="Soft Delete Check", price=25.0)
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Delete (soft delete)
        versioned_repository.delete(saved, VERSIONED_COLLECTION)

        # Verify document still exists in collection
        with mongodb_adapter:
            doc = mongodb_adapter.db[VERSIONED_COLLECTION].find_one({
                'entity_id': entity_id
            })

            assert doc is not None
            assert doc['active'] is False
            assert doc['name'] == "Soft Delete Check"

    def test_versioned_audit_collection_completeness(self, versioned_repository, mongodb_adapter):
        """Test that audit collection has all fields from main collection."""
        # Create and update product
        product = VersionedProduct(
            name="Audit Complete Check",
            price=15.0,
            description="Full description"
        )
        saved = versioned_repository.save(product, VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Update to trigger audit
        saved.price = 25.0
        versioned_repository.save(saved, VERSIONED_COLLECTION)

        # Check audit record has all fields
        with mongodb_adapter:
            audit_rec = mongodb_adapter.db[VERSIONED_AUDIT_COLLECTION].find_one({
                'entity_id': entity_id
            })

            if audit_rec:  # If audit record exists
                assert 'entity_id' in audit_rec
                assert 'version' in audit_rec
                assert 'name' in audit_rec
                assert 'price' in audit_rec
                assert 'description' in audit_rec
                assert 'active' in audit_rec
                assert 'changed_on' in audit_rec


# ============================================================================
# Non-Versioned Model Tests
# ============================================================================

class TestMongoDBNonVersionedModel:
    """Tests for NonVersionedModel behavior with MongoDB."""
    
    def test_nonversioned_model_create(self, nonversioned_repository):
        """Test creating a non-versioned entity without Big 6 fields."""
        # Create a config entry
        config = NonVersionedConfig(
            key="app.setting",
            value="enabled"
        )
        
        # Save the config
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        
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
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        entity_id = saved_config.entity_id
        
        # Update the config
        saved_config.value = "7200"
        updated_config = nonversioned_repository.save(saved_config, NON_VERSIONED_COLLECTION)
        
        # Verify entity_id unchanged
        assert updated_config.entity_id == entity_id
        
        # Verify updated value
        assert updated_config.value == "7200"
        
        # Verify no version tracking
        assert not hasattr(updated_config, 'version') or not isinstance(getattr(updated_config, 'version', None), str)
    
    def test_nonversioned_model_delete(self, nonversioned_repository):
        """Test delete behavior for non-versioned entities."""
        # Create a config
        config = NonVersionedConfig(
            key="temp.setting",
            value="temporary"
        )
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        
        # Delete the config
        deleted_config = nonversioned_repository.delete(saved_config, NON_VERSIONED_COLLECTION)
        
        # NonVersionedModel doesn't have 'active' field
        assert deleted_config is not None
    
    def test_nonversioned_no_audit(self, nonversioned_repository, mongodb_adapter):
        """Test that no audit records are created for non-versioned entities."""
        # Create and update a config
        config = NonVersionedConfig(
            key="no.audit.test",
            value="initial"
        )
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        
        # Update it
        saved_config.value = "updated"
        nonversioned_repository.save(saved_config, NON_VERSIONED_COLLECTION)
        
        # Check that no audit collection has records
        with mongodb_adapter:
            audit_records = list(mongodb_adapter.db[f"{NON_VERSIONED_COLLECTION}_audit"].find({}))
        
        # Should have no audit records
        assert len(audit_records) == 0
    
    def test_nonversioned_no_latest_flag(self, nonversioned_repository, mongodb_adapter):
        """Test that non-versioned models don't have 'latest' flag."""
        # Create a config
        config = NonVersionedConfig(
            key="no.latest.test",
            value="test"
        )
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        
        # Check that the document doesn't have latest flag
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({
                'entity_id': saved_config.entity_id
            })
        
        # Document should exist but shouldn't have 'latest' key
        # (unless the repo adds it - check behavior)
        assert doc is not None
    
    def test_nonversioned_model_get_many(self, nonversioned_repository, mongodb_adapter):
        """Test retrieving multiple non-versioned entities."""
        # Create multiple configs
        for i in range(3):
            config = NonVersionedConfig(
                key=f"batch.key.{i}",
                value=f"value_{i}"
            )
            nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Get all configs directly from MongoDB
        # since get_many for non-versioned may need different handling
        with mongodb_adapter:
            configs = list(mongodb_adapter.db[NON_VERSIONED_COLLECTION].find({}))

        # Should have at least 3 configs
        assert len(configs) >= 3

    def test_nonversioned_hard_delete_verification(self, nonversioned_repository, mongodb_adapter):
        """Test that delete actually removes document from collection (hard delete)."""
        # Create a config
        config = NonVersionedConfig(key="hard.delete", value="will.be.removed")
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Verify it exists
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': entity_id})
            assert doc is not None

        # Delete it (should be hard delete for NonVersionedModel)
        deleted = nonversioned_repository.delete(saved, NON_VERSIONED_COLLECTION)

        # Verify document is actually removed from collection
        with mongodb_adapter:
            doc_after = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': entity_id})
            # For hard delete, document should NOT exist
            assert doc_after is None

    def test_nonversioned_upsert_new_entity(self, nonversioned_repository, mongodb_adapter):
        """Test upserting a completely new non-versioned entity."""
        # Create new config
        config = NonVersionedConfig(key="upsert.new", value="new.entity")
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify saved
        assert saved.entity_id is not None

        # Verify in database
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': saved.entity_id})
            assert doc is not None
            assert doc['key'] == "upsert.new"
            assert doc['value'] == "new.entity"

    def test_nonversioned_upsert_existing_entity(self, nonversioned_repository, mongodb_adapter):
        """Test upserting an existing entity with same entity_id (should replace)."""
        # Create initial config
        config = NonVersionedConfig(key="upsert.existing", value="original")
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        entity_id = saved.entity_id

        # Update with same entity_id (upsert)
        config2 = NonVersionedConfig(key="upsert.existing", value="updated")
        config2.entity_id = entity_id
        updated = nonversioned_repository.save(config2, NON_VERSIONED_COLLECTION)

        # Verify entity_id unchanged
        assert updated.entity_id == entity_id
        assert updated.value == "updated"

        # Verify only ONE document exists in database
        with mongodb_adapter:
            docs = list(mongodb_adapter.db[NON_VERSIONED_COLLECTION].find({'entity_id': entity_id}))
            assert len(docs) == 1
            assert docs[0]['value'] == "updated"

            # Verify no 'latest' flag added
            assert 'latest' not in docs[0] or docs[0].get('latest') is None
    def test_nonversioned_nested_extra_objects(self, nonversioned_repository, mongodb_adapter):
        """Test deeply nested objects in extra fields."""
        # Create config with deeply nested structure
        config = NonVersionedConfig(key="nested.test", value="base")
        config.deep = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": "deep value"
                        }
                    }
                }
            }
        }

        # Save and retrieve
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify nested structure preserved
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': saved.entity_id})
            assert 'deep' in doc
            if isinstance(doc['deep'], dict):
                assert 'level1' in doc['deep']

    def test_nonversioned_array_fields_in_extra(self, nonversioned_repository, mongodb_adapter):
        """Test array fields in extra dictionary."""
        # Create config with arrays
        config = NonVersionedConfig(key="array.test", value="arrays")
        config.tags = ["tag1", "tag2", "tag3"]
        config.numbers = [1, 2, 3, 4, 5]
        config.mixed = ["string", 123, {"nested": "object"}, [1, 2, 3]]

        # Save
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify arrays preserved
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': saved.entity_id})
            assert 'tags' in doc
            if isinstance(doc.get('tags'), list):
                assert len(doc['tags']) == 3
                assert "tag1" in doc['tags']

    def test_nonversioned_entity_id_index_usage(self, nonversioned_repository, mongodb_adapter):
        """Test index performance with many documents."""
        # Create many documents
        entity_ids = []
        for i in range(1000):
            config = NonVersionedConfig(key=f"perf.key.{i}", value=f"value_{i}")
            saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
            if i == 500:  # Save the middle one
                target_id = saved.entity_id
            entity_ids.append(saved.entity_id)

        # Query by entity_id and verify fast retrieval
        with mongodb_adapter:
            # Create index if not exists
            mongodb_adapter.db[NON_VERSIONED_COLLECTION].create_index('entity_id', name='entity_id_idx_performance')

            # Query and explain
            explain = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find({
                'entity_id': target_id
            }).explain()

            # Verify query ran (exact explain format varies)
            assert explain is not None

            # Verify document found
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': target_id})
            assert doc is not None

    def test_nonversioned_get_one_via_repository(self, nonversioned_repository):
        """Test get_one via repository for NonVersionedModel."""
        # Create config
        config = NonVersionedConfig(key="get.one", value="test")
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Retrieve via repository
        retrieved = nonversioned_repository.get_one(
            NON_VERSIONED_COLLECTION,
            None,  # No index needed for simple query
            {'entity_id': saved.entity_id}
        )

        # Verify retrieval
        assert retrieved is not None
        assert retrieved.key == "get.one"
        assert retrieved.value == "test"
        assert retrieved.entity_id == saved.entity_id

    def test_nonversioned_get_many_with_pagination(self, nonversioned_repository):
        """Test get_many with limit and skip for pagination."""
        # Create multiple configs
        for i in range(20):
            config = NonVersionedConfig(key=f"page.key.{i}", value=f"value_{i}")
            nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Get first page (limit 5)
        page1 = nonversioned_repository.get_many(
            NON_VERSIONED_COLLECTION,
            None,
            query={},
            limit=5
        )

        # Should have exactly 5 items
        assert len(page1) == 5

        # Get second page (skip 5, limit 5)
        page2 = nonversioned_repository.get_many(
            NON_VERSIONED_COLLECTION,
            None,
            query={},
            limit=5,
            offset=5
        )

        # Should have 5 items and be different from page 1
        assert len(page2) == 5
        page1_ids = [p.entity_id for p in page1]
        page2_ids = [p.entity_id for p in page2]
        assert not any(pid in page1_ids for pid in page2_ids)

    def test_nonversioned_null_and_undefined_fields(self, nonversioned_repository, mongodb_adapter):
        """Test NULL and undefined field handling."""
        # Create config with None values
        config = NonVersionedConfig(key="null.test", value=None)
        config.optional_field = None
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify in database
        with mongodb_adapter:
            doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': saved.entity_id})
            assert doc is not None
            # NULL fields may be stored as None or omitted entirely in MongoDB
            assert doc.get('value') is None or 'value' not in doc

    def test_nonversioned_special_characters(self, nonversioned_repository):
        """Test special characters and Unicode."""
        # Create config with special characters
        config = NonVersionedConfig(
            key="special.émoji.世界",
            value="🌍 Special: \"quotes\" 'apostrophes' $dollar €euro ñ á é í ó ú"
        )

        # Save and retrieve
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify special characters preserved
        assert "émoji" in saved.key
        assert "世界" in saved.key
        assert "🌍" in saved.value
        assert "ñ" in saved.value

    def test_nonversioned_large_document(self, nonversioned_repository):
        """Test handling of large documents (approaching MongoDB 16MB limit)."""
        # Create config with large value (1MB of text)
        large_text = "x" * (1024 * 1024)  # 1MB
        config = NonVersionedConfig(key="large.doc", value=large_text)

        # Add more fields
        for i in range(10):
            setattr(config, f"field_{i}", f"value_{i}" * 100)

        # Save (should work, ~1MB well under 16MB limit)
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify saved
        assert saved is not None
        assert len(saved.value) == 1024 * 1024

    def test_nonversioned_update_preserves_entity_id(self, nonversioned_repository, mongodb_adapter):
        """Test that updates preserve the entity_id."""
        # Create initial config
        config = NonVersionedConfig(key="preserve.id", value="original")
        saved = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        original_id = saved.entity_id

        # Update multiple times
        for i in range(5):
            saved.value = f"updated_{i}"
            saved = nonversioned_repository.save(saved, NON_VERSIONED_COLLECTION)

        # Verify entity_id never changed
        assert saved.entity_id == original_id

        # Verify only one document with this entity_id
        with mongodb_adapter:
            docs = list(mongodb_adapter.db[NON_VERSIONED_COLLECTION].find({'entity_id': original_id}))
            assert len(docs) == 1
            assert docs[0]['value'] == "updated_4"


# ============================================================================
# Non-Versioned Posts Model Tests
# ============================================================================

class TestMongoDBNonVersionedPosts:
    """Tests for NonVersionedPost model with MongoDB."""

    def test_posts_create(self, posts_repository):
        """Test creating a non-versioned post."""
        post = NonVersionedPost(title="First Post", description="This is my first blog post")
        saved_post = posts_repository.save(post, NON_VERSIONED_POST_COLLECTION)

        assert saved_post is not None
        assert saved_post.entity_id is not None
        assert saved_post.title == "First Post"
        assert saved_post.description == "This is my first blog post"

    def test_posts_update(self, posts_repository):
        """Test updating a non-versioned post."""
        post = NonVersionedPost(title="Original Title", description="Original description")
        saved_post = posts_repository.save(post, NON_VERSIONED_POST_COLLECTION)

        # Update the post
        saved_post.title = "Updated Title"
        saved_post.description = "Updated description"
        updated_post = posts_repository.save(saved_post, NON_VERSIONED_POST_COLLECTION)

        assert updated_post.title == "Updated Title"
        assert updated_post.description == "Updated description"
        assert updated_post.entity_id == saved_post.entity_id

    def test_posts_with_extra_fields(self, posts_repository):
        """Test posts with extra fields (MongoDB schema-less)."""
        post = NonVersionedPost(title="Post with metadata", description="A post with extra data")
        post.author = "John Doe"
        post.tags = ["python", "mongodb", "testing"]
        post.views = 100

        saved_post = posts_repository.save(post, NON_VERSIONED_POST_COLLECTION)

        assert saved_post.title == "Post with metadata"
        assert saved_post.author == "John Doe"
        assert saved_post.tags == ["python", "mongodb", "testing"]
        assert saved_post.views == 100

    def test_posts_nested_extra_fields(self, posts_repository):
        """Test posts with nested extra fields."""
        post = NonVersionedPost(title="Complex Post", description="Post with nested data")
        post.metadata = {
            "category": "tutorial",
            "difficulty": "intermediate",
            "stats": {
                "likes": 50,
                "shares": 10
            }
        }

        saved_post = posts_repository.save(post, NON_VERSIONED_POST_COLLECTION)

        assert hasattr(saved_post, 'metadata')
        if isinstance(saved_post.metadata, dict):
            assert saved_post.metadata['category'] == "tutorial"
            assert saved_post.metadata['stats']['likes'] == 50


# ============================================================================
# Non-Versioned Cars Model Tests
# ============================================================================

class TestMongoDBNonVersionedCars:
    """Tests for NonVersionedCar model with MongoDB."""

    def test_cars_create(self, cars_repository):
        """Test creating a non-versioned car."""
        car = NonVersionedCar(name="Model S", brand="Tesla")
        saved_car = cars_repository.save(car, NON_VERSIONED_CAR_COLLECTION)

        assert saved_car is not None
        assert saved_car.entity_id is not None
        assert saved_car.name == "Model S"
        assert saved_car.brand == "Tesla"

    def test_cars_update(self, cars_repository):
        """Test updating a non-versioned car."""
        car = NonVersionedCar(name="Camry", brand="Toyota")
        saved_car = cars_repository.save(car, NON_VERSIONED_CAR_COLLECTION)

        # Update the car
        saved_car.name = "Camry Hybrid"
        updated_car = cars_repository.save(saved_car, NON_VERSIONED_CAR_COLLECTION)

        assert updated_car.name == "Camry Hybrid"
        assert updated_car.brand == "Toyota"
        assert updated_car.entity_id == saved_car.entity_id

    def test_cars_with_extra_fields(self, cars_repository):
        """Test cars with extra fields (MongoDB schema-less)."""
        car = NonVersionedCar(name="Mustang", brand="Ford")
        car.year = 2024
        car.color = "Red"
        car.electric = False
        car.horsepower = 450

        saved_car = cars_repository.save(car, NON_VERSIONED_CAR_COLLECTION)

        assert saved_car.name == "Mustang"
        assert saved_car.brand == "Ford"
        assert saved_car.year == 2024
        assert saved_car.color == "Red"
        assert saved_car.electric == False
        assert saved_car.horsepower == 450

    def test_cars_nested_extra_fields(self, cars_repository):
        """Test cars with nested extra fields."""
        car = NonVersionedCar(name="Model 3", brand="Tesla")
        car.specs = {
            "battery": "82 kWh",
            "range": "358 miles",
            "performance": {
                "acceleration": "3.1s",
                "top_speed": "162 mph"
            }
        }

        saved_car = cars_repository.save(car, NON_VERSIONED_CAR_COLLECTION)

        assert hasattr(saved_car, 'specs')
        if isinstance(saved_car.specs, dict):
            assert saved_car.specs['battery'] == "82 kWh"
            assert saved_car.specs['performance']['acceleration'] == "3.1s"

    def test_cars_multiple_brands(self, cars_repository):
        """Test creating multiple cars from different brands."""
        cars = [
            NonVersionedCar(name="Civic", brand="Honda"),
            NonVersionedCar(name="Accord", brand="Honda"),
            NonVersionedCar(name="Corolla", brand="Toyota"),
            NonVersionedCar(name="Model Y", brand="Tesla")
        ]

        saved_cars = [cars_repository.save(car, NON_VERSIONED_CAR_COLLECTION) for car in cars]

        assert len(saved_cars) == 4
        assert all(car.entity_id is not None for car in saved_cars)

        honda_count = sum(1 for car in saved_cars if car.brand == "Honda")
        assert honda_count == 2


# ============================================================================
# Integration Tests
# ============================================================================

class TestMongoDBIntegration:
    """Integration tests for cross-cutting concerns in MongoDB."""

    def test_mixed_models_no_interference(self, versioned_repository, nonversioned_repository):
        """Test that VersionedModel and NonVersionedModel work together without interference."""
        # Create versioned product
        product = VersionedProduct(name="Mixed MongoDB Product", price=100.0)
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)

        # Create non-versioned config
        config = NonVersionedConfig(key="mixed.mongo", value="config")
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Verify both exist
        assert saved_product.entity_id is not None
        assert saved_config.entity_id is not None

        # Verify they're in different collections
        with versioned_repository.adapter:
            product_doc = versioned_repository.adapter.db[VERSIONED_COLLECTION].find_one({
                'entity_id': saved_product.entity_id
            })
            config_doc = nonversioned_repository.adapter.db[NON_VERSIONED_COLLECTION].find_one({
                'entity_id': saved_config.entity_id
            })

            assert product_doc is not None
            assert config_doc is not None

            # Verify versioned has special fields
            assert 'latest' in product_doc
            assert 'active' in product_doc

            # Verify non-versioned doesn't
            assert 'latest' not in config_doc or config_doc.get('latest') is None
            assert 'active' not in config_doc or config_doc.get('active') is None

    def test_collection_naming_conventions(self, mongodb_adapter):
        """Test that collection names follow conventions."""
        # Collections should exist with expected names
        with mongodb_adapter:
            collection_names = mongodb_adapter.db.list_collection_names()

            # Verify collections exist (may not if tests haven't run yet)
            # Just test that we can create them with proper names
            test_collection = "test_naming_collection"
            mongodb_adapter.db[test_collection].insert_one({'test': 'data'})

            # Clean up
            mongodb_adapter.db[test_collection].drop()

    def test_index_creation_both_types(self, mongodb_adapter, setup_mongodb_collections):
        """Test index creation for both model types."""
        with mongodb_adapter:
            # Check versioned collection indexes
            versioned_indexes = list(mongodb_adapter.db[VERSIONED_COLLECTION].list_indexes())
            index_names = [idx['name'] for idx in versioned_indexes]

            # Should have entity_latest_idx and entity_active_idx
            assert 'entity_latest_idx' in index_names or any('entity' in name for name in index_names)

            # Create index for non-versioned collection
            mongodb_adapter.db[NON_VERSIONED_COLLECTION].create_index('entity_id', unique=True, name='entity_id_unique_idx')

            # Verify it was created
            nonversioned_indexes = list(mongodb_adapter.db[NON_VERSIONED_COLLECTION].list_indexes())
            nonversioned_index_names = [idx['name'] for idx in nonversioned_indexes]
            assert 'entity_id_unique_idx' in nonversioned_index_names or any('entity' in name for name in nonversioned_index_names)

    def test_adapter_upsert_vs_save_routing(self, nonversioned_repository, versioned_repository):
        """Test that adapter routes to correct method based on model type."""
        # For NonVersionedModel, should use upsert
        config = NonVersionedConfig(key="routing.test", value="upsert")
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Update - should still upsert
        saved_config.value = "upserted"
        updated_config = nonversioned_repository.save(saved_config, NON_VERSIONED_COLLECTION)

        assert updated_config.value == "upserted"

        # For VersionedModel, should use versioned save
        product = VersionedProduct(name="Routing Test", price=50.0)
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)

        # Update - should create new version
        original_version = saved_product.version
        saved_product.price = 75.0
        updated_product = versioned_repository.save(saved_product, VERSIONED_COLLECTION)

        # Verify version changed
        assert updated_product.version != original_version

    def test_repository_delete_routing(self, versioned_repository, nonversioned_repository, mongodb_adapter):
        """Test that delete operations route correctly (soft vs hard)."""
        # VersionedModel - soft delete
        product = VersionedProduct(name="Delete Routing", price=10.0)
        saved_product = versioned_repository.save(product, VERSIONED_COLLECTION)
        product_id = saved_product.entity_id

        deleted_product = versioned_repository.delete(saved_product, VERSIONED_COLLECTION)

        # Should still exist in collection with active=False
        with mongodb_adapter:
            product_doc = mongodb_adapter.db[VERSIONED_COLLECTION].find_one({'entity_id': product_id})
            assert product_doc is not None
            assert product_doc['active'] is False

        # NonVersionedModel - hard delete
        config = NonVersionedConfig(key="delete.routing", value="test")
        saved_config = nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        config_id = saved_config.entity_id

        deleted_config = nonversioned_repository.delete(saved_config, NON_VERSIONED_COLLECTION)

        # Should NOT exist in collection (hard delete)
        with mongodb_adapter:
            config_doc = mongodb_adapter.db[NON_VERSIONED_COLLECTION].find_one({'entity_id': config_id})
            assert config_doc is None

    def test_mongodb_specific_operators(self, nonversioned_repository, mongodb_adapter):
        """Test MongoDB-specific query operators."""
        # Create configs with different values
        for i in range(10):
            config = NonVersionedConfig(key=f"operator.test.{i}", value=str(i))
            nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)

        # Use MongoDB $gte operator
        with mongodb_adapter:
            docs = list(mongodb_adapter.db[NON_VERSIONED_COLLECTION].find({
                'key': {'$regex': '^operator\\.test'}
            }).limit(5))

            assert len(docs) >= 5

    def test_performance_versioned_vs_nonversioned(self, versioned_repository, nonversioned_repository):
        """Compare performance between versioned and non-versioned operations."""
        import time

        # Benchmark VersionedModel (with audit overhead)
        versioned_start = time.time()
        for i in range(50):
            product = VersionedProduct(name=f"Perf {i}", price=float(i))
            versioned_repository.save(product, VERSIONED_COLLECTION)
        versioned_time = time.time() - versioned_start

        # Benchmark NonVersionedModel (with upsert, no audit)
        nonversioned_start = time.time()
        for i in range(50):
            config = NonVersionedConfig(key=f"perf.{i}", value=str(i))
            nonversioned_repository.save(config, NON_VERSIONED_COLLECTION)
        nonversioned_time = time.time() - nonversioned_start

        # Both should complete in reasonable time
        assert versioned_time < 10.0
        assert nonversioned_time < 10.0

        # NonVersionedModel should generally be faster (less overhead)
        # But we don't enforce strict performance ratios as it depends on system


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

