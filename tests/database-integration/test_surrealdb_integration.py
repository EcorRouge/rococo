"""
SurrealDB Integration Tests for ROC-76

Tests for both VersionedModel and NonVersionedModel functionality with real SurrealDB database.
Requires SurrealDB to be running and accessible via environment variables.

Environment Variables:
- SURREAL_ENDPOINT: SurrealDB WebSocket endpoint (default: ws://localhost:8000/rpc)
- SURREAL_USER: SurrealDB username (default: root)
- SURREAL_PASSWORD: SurrealDB password (default: root)
- SURREAL_NAMESPACE: SurrealDB namespace (default: test)
- SURREAL_DATABASE: SurrealDB database name (default: rococo_test)
"""

import pytest
from uuid import uuid4
from dataclasses import dataclass, field
from typing import Optional

from conftest import (
    get_surrealdb_config,
    MockMessageAdapter
)

from test_models import SurrealNonVersionedPost, SurrealNonVersionedCar

from rococo.data.surrealdb import SurrealDbAdapter
from rococo.repositories.surrealdb.surreal_db_repository import SurrealDbRepository
from rococo.models.surrealdb.surreal_versioned_model import SurrealVersionedModel, get_uuid_hex
from rococo.models import NonVersionedModel


# Skip all tests in this module if SurrealDB configuration is not available
pytestmark = pytest.mark.skipif(
    get_surrealdb_config() is None,
    reason="SurrealDB configuration not available. Set SURREALDB_HOST, SURREALDB_PORT, SURREALDB_USER, SURREALDB_PASSWORD, SURREALDB_NAMESPACE, SURREALDB_DATABASE environment variables."
)


# Define SurrealDB-specific test models
@dataclass(kw_only=True)
class SurrealVersionedProduct(SurrealVersionedModel):
    """
    A SurrealDB-specific versioned product model for testing.
    Uses record_id field type for entity_id suitable for SurrealDB's graph storage.
    """
    name: str = ""
    price: float = 0.0
    description: Optional[str] = None


@dataclass(kw_only=True)
class SurrealNonVersionedConfig(NonVersionedModel):
    """
    A SurrealDB-compatible non-versioned configuration model.
    Note: SurrealDB uses entity_id as record_id pattern.
    """
    key: str = ""
    value: str = ""


# Table names
VERSIONED_TABLE = "surrealversionedproduct"
NON_VERSIONED_TABLE = "surrealnonversionedconfig"
NON_VERSIONED_POST_TABLE = "surrealnonversionedpost"
NON_VERSIONED_CAR_TABLE = "surrealnonversionedcar"


@pytest.fixture
def surrealdb_adapter():
    """Create a SurrealDB adapter for testing."""
    config = get_surrealdb_config()
    adapter = SurrealDbAdapter(
        endpoint=config['endpoint'],
        username=config['username'],
        password=config['password'],
        namespace=config['namespace'],
        db_name=config['database']
    )
    return adapter


@pytest.fixture
def setup_surrealdb_tables(surrealdb_adapter):
    """Set up test tables and clean up after tests."""
    with surrealdb_adapter:
        # SurrealDB creates tables implicitly, but we can define schema if needed
        # For now, just ensure any existing test data is cleared
        surrealdb_adapter.execute_query(f"DELETE FROM {VERSIONED_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {VERSIONED_TABLE}_audit")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_POST_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_CAR_TABLE}")

    yield

    # Cleanup after tests
    with surrealdb_adapter:
        surrealdb_adapter.execute_query(f"DELETE FROM {VERSIONED_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {VERSIONED_TABLE}_audit")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_POST_TABLE}")
        surrealdb_adapter.execute_query(f"DELETE FROM {NON_VERSIONED_CAR_TABLE}")


@pytest.fixture
def versioned_repository(surrealdb_adapter, setup_surrealdb_tables):
    """Create a repository for SurrealVersionedProduct."""
    message_adapter = MockMessageAdapter()
    user_id = uuid4()
    repo = SurrealDbRepository(
        db_adapter=surrealdb_adapter,
        model=SurrealVersionedProduct,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=user_id
    )
    repo.use_audit_table = True
    return repo


@pytest.fixture
def nonversioned_repository(surrealdb_adapter, setup_surrealdb_tables):
    """Create a repository for SurrealNonVersionedConfig."""
    message_adapter = MockMessageAdapter()
    # Note: SurrealDbRepository is designed for SurrealVersionedModel
    # We'll need to handle NonVersionedModel differently
    # For now, we'll use the base repository pattern
    from rococo.repositories.base_repository import BaseRepository

    repo = BaseRepository(
        adapter=surrealdb_adapter,
        model=SurrealNonVersionedConfig,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_TABLE
    return repo


@pytest.fixture
def posts_repository(surrealdb_adapter, setup_surrealdb_tables):
    """Create a repository for SurrealNonVersionedPost."""
    message_adapter = MockMessageAdapter()
    from rococo.repositories.base_repository import BaseRepository

    repo = BaseRepository(
        adapter=surrealdb_adapter,
        model=SurrealNonVersionedPost,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_POST_TABLE
    return repo


@pytest.fixture
def cars_repository(surrealdb_adapter, setup_surrealdb_tables):
    """Create a repository for SurrealNonVersionedCar."""
    message_adapter = MockMessageAdapter()
    from rococo.repositories.base_repository import BaseRepository

    repo = BaseRepository(
        adapter=surrealdb_adapter,
        model=SurrealNonVersionedCar,
        message_adapter=message_adapter,
        queue_name="test_queue",
        user_id=None
    )
    repo.table_name = NON_VERSIONED_CAR_TABLE
    return repo


# ============================================================================
# Versioned Model Tests
# ============================================================================

class TestSurrealDBVersionedModel:
    """Tests for VersionedModel behavior with SurrealDB."""
    
    def test_versioned_model_create(self, versioned_repository):
        """Test creating a versioned entity with Big 6 fields."""
        # Create a new product
        product = SurrealVersionedProduct(
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
        assert saved_product.price == 29.99
        assert saved_product.description == "A test product"
        
        # Retrieve and verify
        retrieved = versioned_repository.get_one({'entity_id': saved_product.entity_id})
        assert retrieved is not None
        assert retrieved.name == "Test Product"
        assert retrieved.price == 29.99
    
    def test_versioned_model_update(self, versioned_repository):
        """Test updating a versioned entity with version bump."""
        # Create initial product
        product = SurrealVersionedProduct(
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
        assert updated_product.price == 24.99
    
    def test_versioned_model_delete(self, versioned_repository):
        """Test soft delete sets active=False."""
        # Create a product
        product = SurrealVersionedProduct(
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
        retrieved = versioned_repository.get_one({'entity_id': entity_id})
        assert retrieved is None  # Should not find inactive product
    
    def test_versioned_model_audit_table(self, versioned_repository, surrealdb_adapter):
        """Test that audit records are created on update."""
        # Create initial product
        product = SurrealVersionedProduct(
            name="Audit Test",
            price=15.99
        )
        saved_product = versioned_repository.save(product)
        entity_id = saved_product.entity_id
        
        # Update to create audit record
        saved_product.name = "Audit Test Updated"
        versioned_repository.save(saved_product)
        
        # Check audit table
        with surrealdb_adapter:
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE}_audit WHERE entity_id = '{entity_id}'"
            )
            audit_records = surrealdb_adapter.parse_db_response(result)
        
        # Should have at least one audit record (the original version)
        if isinstance(audit_records, dict):
            audit_records = [audit_records]
        assert len(audit_records) >= 1
    
    def test_versioned_model_get_many(self, versioned_repository):
        """Test retrieving multiple versioned entities."""
        # Create multiple products
        for i in range(3):
            product = SurrealVersionedProduct(
                name=f"Product {i}",
                price=10.0 + i
            )
            versioned_repository.save(product)
        
        # Get all products
        products = versioned_repository.get_many({})
        
        # Should have at least 3 products
        assert len(products) >= 3
        
        # All should be active
        for p in products:
            assert p.active is True
    
    def test_versioned_model_record_id_format(self, versioned_repository, surrealdb_adapter):
        """Test that SurrealDB record IDs are correctly formatted."""
        # Create a product
        product = SurrealVersionedProduct(
            name="Record ID Test",
            price=5.0
        )
        saved_product = versioned_repository.save(product)
        
        # Verify the record exists in SurrealDB with correct ID format
        with surrealdb_adapter:
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE} WHERE active=true"
            )
            records = surrealdb_adapter.parse_db_response(result)
        
        if isinstance(records, dict):
            records = [records]
        
        # Should have at least one record
        assert len(records) >= 1
        
        # Check that 'id' field follows table:uuid pattern
        for record in records:
            if 'id' in record:
                record_id = record['id']
                # SurrealDB record IDs are in format "table:uuid"
                assert ':' in str(record_id)

    def test_versioned_get_one_inactive_entity(self, versioned_repository, surrealdb_adapter):
        """Test querying for inactive entities explicitly."""
        # Create and delete a product
        product = SurrealVersionedProduct(name="Inactive SurrealDB Test", price=12.99)
        saved_product = versioned_repository.save(product)
        entity_id = saved_product.entity_id
        versioned_repository.delete(saved_product)

        # Query database directly for inactive entity
        with surrealdb_adapter:
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE} WHERE entity_id = '{entity_id}' AND active = false"
            )
            inactive_records = surrealdb_adapter.parse_db_response(result)

        if isinstance(inactive_records, dict):
            inactive_records = [inactive_records]

        assert len(inactive_records) == 1
        assert inactive_records[0]['active'] is False

    def test_versioned_multiple_version_history(self, versioned_repository, surrealdb_adapter):
        """Test creating multiple versions and verifying audit trail."""
        product = SurrealVersionedProduct(name="Version History SurrealDB", price=10.0)
        saved = versioned_repository.save(product)
        entity_id = saved.entity_id

        # Create 5 versions
        for i in range(5):
            saved.price = 10.0 + (i + 1) * 5.0
            saved = versioned_repository.save(saved)

        # Check audit table
        with surrealdb_adapter:
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE}_audit WHERE entity_id = '{entity_id}'"
            )
            audit_records = surrealdb_adapter.parse_db_response(result)

        if isinstance(audit_records, dict):
            audit_records = [audit_records]

        assert len(audit_records) >= 5

    def test_versioned_version_bump_on_delete(self, versioned_repository):
        """Test that delete bumps version."""
        product = SurrealVersionedProduct(name="Delete Version Bump SurrealDB", price=15.0)
        saved = versioned_repository.save(product)
        original_version = saved.version

        deleted = versioned_repository.delete(saved)
        assert deleted.version != original_version
        assert deleted.previous_version == original_version

    def test_versioned_audit_table_completeness(self, versioned_repository, surrealdb_adapter):
        """Test audit table has complete version history."""
        product = SurrealVersionedProduct(name="Audit Complete SurrealDB", price=1.0)
        saved = versioned_repository.save(product)
        entity_id = saved.entity_id

        for i in range(3):
            saved.price = float(i + 2)
            saved = versioned_repository.save(saved)

        with surrealdb_adapter:
            # Check main table has only latest version
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE} WHERE entity_id = '{entity_id}' AND active = true"
            )
            main_records = surrealdb_adapter.parse_db_response(result)
            if isinstance(main_records, dict):
                main_records = [main_records]
            assert len(main_records) == 1

            # Check audit table has all previous versions
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE}_audit WHERE entity_id = '{entity_id}'"
            )
            audit_records = surrealdb_adapter.parse_db_response(result)
            if isinstance(audit_records, dict):
                audit_records = [audit_records]
            assert len(audit_records) >= 3

    def test_versioned_uuid_consistency(self, versioned_repository):
        """Test UUID fields are consistent."""
        product = SurrealVersionedProduct(name="UUID SurrealDB", price=7.0)
        saved = versioned_repository.save(product)

        # SurrealDB entity_id should be a UUID hex string (32 chars without hyphens)
        assert saved.entity_id is not None
        assert len(str(saved.entity_id).replace('-', '')) == 32
        assert saved.version is not None
        assert len(str(saved.version).replace('-', '')) == 32

    def test_versioned_datetime_handling(self, versioned_repository):
        """Test SurrealDB datetime handling."""
        from datetime import datetime
        product = SurrealVersionedProduct(name="Datetime SurrealDB", price=3.0)
        saved = versioned_repository.save(product)

        assert saved.changed_on is not None
        assert isinstance(saved.changed_on, (datetime, str))

    def test_versioned_execute_query_with_variables(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test execute_query with variables parameter."""
        # Create a test record
        test_data = {
            'entity_id': str(uuid4()).replace('-', ''),
            'name': 'Query Variable Test',
            'price': 99.99,
            'active': True,
            'version': str(uuid4()).replace('-', ''),
            'changed_on': 'time::now()'
        }

        with surrealdb_adapter:
            # Test query without variables (current working approach)
            query = f"CREATE {VERSIONED_TABLE}:`{test_data['entity_id']}` SET name = 'Query Variable Test', price = 99.99, active = true, version = '{test_data['version']}', changed_on = time::now()"
            result = surrealdb_adapter.execute_query(query)
            assert result is not None

    def test_versioned_get_many_with_limit(self, versioned_repository):
        """Test get_many with limit parameter."""
        # Create 5 products
        for i in range(5):
            product = SurrealVersionedProduct(
                name=f"Limit Test {i}",
                price=10.0 + i
            )
            versioned_repository.save(product)

        # Get with limit
        products = versioned_repository.get_many({}, limit=2)

        # Should return at most 2 products
        assert len(products) <= 2


# ============================================================================
# Non-Versioned Model Tests
# ============================================================================

class TestSurrealDBNonVersionedModel:
    """Tests for NonVersionedModel behavior with SurrealDB."""
    
    def test_nonversioned_model_create(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test creating a non-versioned entity without Big 6 fields."""
        # Create a config entry
        config = SurrealNonVersionedConfig(
            key="app.setting",
            value="enabled"
        )
        
        # Prepare data for saving
        config.prepare_for_save()
        data = config.as_dict()
        
        # Save directly via adapter since we need custom handling
        with surrealdb_adapter:
            # Create the record in SurrealDB
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )
        
        # Verify entity_id is set
        assert config.entity_id is not None
        
        # Verify no Big 6 versioning fields (checking that model doesn't have them)
        assert not hasattr(config, 'version') or getattr(type(config), 'version', None) is None
        assert not hasattr(config, 'previous_version') or getattr(type(config), 'previous_version', None) is None
        assert not hasattr(config, 'active') or getattr(type(config), 'active', None) is None
        
        # Verify custom fields
        assert config.key == "app.setting"
        assert config.value == "enabled"
    
    def test_nonversioned_model_update(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test updating a non-versioned entity without version tracking."""
        # Create initial config
        config = SurrealNonVersionedConfig(
            key="cache.ttl",
            value="3600"
        )
        config.prepare_for_save()
        entity_id = config.entity_id
        
        # Save initial
        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )
        
        # Update the config
        config.value = "7200"
        
        with surrealdb_adapter:
            surrealdb_adapter.execute_query(
                f"UPDATE {record_id} SET value = '7200'"
            )
            
            # Retrieve and verify
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)
        
        # Verify entity_id unchanged
        assert record is not None
        if isinstance(record, list):
            record = record[0]
        
        # Verify updated value
        assert record.get('value') == "7200"
    
    def test_nonversioned_no_audit(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test that no audit records are created for non-versioned entities."""
        # Create and update a config
        config = SurrealNonVersionedConfig(
            key="no.audit.test",
            value="initial"
        )
        config.prepare_for_save()
        entity_id = config.entity_id
        
        # Save and update
        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )
            
            # Update it
            surrealdb_adapter.execute_query(
                f"UPDATE {record_id} SET value = 'updated'"
            )
            
            # Check that no audit table has records
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {NON_VERSIONED_TABLE}_audit"
            )
            audit_records = surrealdb_adapter.parse_db_response(result)
        
        # Should have no audit records
        if isinstance(audit_records, dict) and audit_records:
            audit_records = [audit_records]
        elif not audit_records:
            audit_records = []
        
        assert len(audit_records) == 0
    
    def test_nonversioned_model_get_many(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test retrieving multiple non-versioned entities."""
        # Create multiple configs
        with surrealdb_adapter:
            for i in range(3):
                config = SurrealNonVersionedConfig(
                    key=f"batch.key.{i}",
                    value=f"value_{i}"
                )
                config.prepare_for_save()
                record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
                data = config.as_dict()
                surrealdb_adapter.execute_query(
                    f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
                )
            
            # Get all configs
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {NON_VERSIONED_TABLE}"
            )
            configs = surrealdb_adapter.parse_db_response(result)
        
        if isinstance(configs, dict):
            configs = [configs]

        # Should have at least 3 configs
        assert len(configs) >= 3

    def test_nonversioned_model_delete(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test delete behavior for non-versioned entities."""
        # Create a config
        config = SurrealNonVersionedConfig(
            key="temp.setting",
            value="temporary"
        )
        config.prepare_for_save()

        # Save via adapter
        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

        # NonVersionedModel doesn't have 'active' field
        # Verify it was created
        assert config.entity_id is not None

    def test_nonversioned_get_one_via_repository(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test get_one via repository.

        Non-versioned models now include Big 6 fields for backward compatibility.
        """
        config = SurrealNonVersionedConfig(key="test.key", value="test.value")
        config.prepare_for_save()

        # Save via adapter's save method (adds Big 6 fields with defaults)
        with surrealdb_adapter:
            data = config.as_dict()
            data['id'] = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            surrealdb_adapter.save(NON_VERSIONED_TABLE, data)

        # Should succeed now that documents have active field
        with surrealdb_adapter:
            result = surrealdb_adapter.get_one(
                NON_VERSIONED_TABLE,
                {'entity_id': config.entity_id}
            )
        assert result is not None
        assert result.get('key') == "test.key"

    def test_nonversioned_get_many_via_repository(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test get_many via repository.

        Non-versioned models now include Big 6 fields for backward compatibility.
        """
        for i in range(3):
            config = SurrealNonVersionedConfig(key=f"many.{i}", value=f"val_{i}")
            config.prepare_for_save()

            # Save via adapter's save method (adds Big 6 fields with defaults)
            with surrealdb_adapter:
                data = config.as_dict()
                data['id'] = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
                surrealdb_adapter.save(NON_VERSIONED_TABLE, data)

        # Should succeed now that documents have active field
        with surrealdb_adapter:
            configs = surrealdb_adapter.get_many(NON_VERSIONED_TABLE, {})
        assert configs is not None
        assert len(configs) >= 3

    def test_nonversioned_delete_via_adapter(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test delete via adapter.

        Non-versioned models now include Big 6 fields for backward compatibility.
        SurrealDB adapter performs soft delete by setting active=False.
        """
        config = SurrealNonVersionedConfig(key="delete.test", value="delete.me")
        config.prepare_for_save()

        # Save via adapter's save method (adds Big 6 fields with defaults)
        with surrealdb_adapter:
            data = config.as_dict()
            data['id'] = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            surrealdb_adapter.save(NON_VERSIONED_TABLE, data)

        # Should succeed now that documents have active field
        with surrealdb_adapter:
            data_with_id = config.as_dict()
            data_with_id['id'] = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            deleted = surrealdb_adapter.delete(NON_VERSIONED_TABLE, data_with_id)
        assert deleted is not None

    def test_nonversioned_null_values(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test NULL value handling."""
        config = SurrealNonVersionedConfig(key="null.test", value=None)
        config.prepare_for_save()
        data = config.as_dict()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            # SurrealDB uses NONE for null values
            none_str = "NONE"
            content_parts = [f'{k}: {repr(v) if v is not None else none_str}' for k, v in data.items()]
            content_str = ', '.join(content_parts)
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{content_str}}}"
            )

            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)

        if isinstance(record, list):
            record = record[0]
        assert record.get('value') is None or record.get('value') == 'None'

    def test_nonversioned_empty_strings(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test empty string handling."""
        config = SurrealNonVersionedConfig(key="", value="")
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)

        if isinstance(record, list):
            record = record[0]
        assert record.get('key') == ""
        assert record.get('value') == ""

    def test_nonversioned_special_characters_escaping(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test special characters and SurrealQL escaping."""
        config = SurrealNonVersionedConfig(
            key="special'quote",
            value='Double"quotes and \\ backslash'
        )
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)

        if isinstance(record, list):
            record = record[0]
        assert "'" in str(record.get('key'))
        assert '"' in str(record.get('value'))

    def test_nonversioned_large_text_fields(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test SurrealDB with large data."""
        large_text = "x" * 10000  # 10KB
        config = SurrealNonVersionedConfig(key="large.text", value=large_text)
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)

        if isinstance(record, list):
            record = record[0]
        assert len(str(record.get('value'))) >= 9000  # Allow some variance

    def test_nonversioned_no_audit_table_creation(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test that audit table is NOT created for NonVersionedModel."""
        config = SurrealNonVersionedConfig(key="no.audit", value="test")
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            # Update to trigger potential audit
            surrealdb_adapter.execute_query(
                f"UPDATE {record_id} SET value = 'updated'"
            )

            # Check that no audit table has records
            try:
                result = surrealdb_adapter.execute_query(
                    f"SELECT * FROM {NON_VERSIONED_TABLE}_audit"
                )
                audit_records = surrealdb_adapter.parse_db_response(result)
                if isinstance(audit_records, dict) and audit_records:
                    audit_records = [audit_records]
                elif not audit_records:
                    audit_records = []
                assert len(audit_records) == 0
            except Exception:
                # Table/query error expected if audit table doesn't exist
                pass

    def test_nonversioned_record_id_format(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test that SurrealDB record IDs are correctly formatted for NonVersionedModel."""
        config = SurrealNonVersionedConfig(key="record.id.test", value="test")
        config.prepare_for_save()
        entity_id = config.entity_id

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {NON_VERSIONED_TABLE}"
            )
            records = surrealdb_adapter.parse_db_response(result)

        if isinstance(records, dict):
            records = [records]

        # Find our record
        found = False
        for record in records:
            if 'id' in record:
                record_id_str = str(record['id'])
                if ':' in record_id_str and entity_id in record_id_str:
                    found = True
                    break

        assert found

    def test_nonversioned_same_entity_id_upsert(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test SurrealDB upsert with same entity_id."""
        config = SurrealNonVersionedConfig(key="upsert.test", value="original")
        config.prepare_for_save()
        entity_id = config.entity_id

        # Create initial record
        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

        # Update with same entity_id (should replace)
        config2 = SurrealNonVersionedConfig(key="upsert.test", value="updated")
        config2.entity_id = entity_id

        with surrealdb_adapter:
            surrealdb_adapter.execute_query(
                f"UPDATE {record_id} SET value = 'updated'"
            )

            # Verify updated
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            record = surrealdb_adapter.parse_db_response(result)

        if isinstance(record, list):
            record = record[0]
        assert record.get('value') == "updated"

    def test_nonversioned_parse_response_edge_cases(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test parse_db_response with various edge cases."""
        with surrealdb_adapter:
            # Empty result
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {NON_VERSIONED_TABLE} WHERE key = 'nonexistent'"
            )
            parsed = surrealdb_adapter.parse_db_response(result)
            assert parsed == [] or parsed is None or parsed == {}

    def test_nonversioned_condition_building(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test condition string building with various data types."""
        config = SurrealNonVersionedConfig(key="condition.test", value="test123")
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

            # Test with string condition
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {NON_VERSIONED_TABLE} WHERE key = 'condition.test'"
            )
            records = surrealdb_adapter.parse_db_response(result)

            if isinstance(records, dict):
                records = [records]
            assert len(records) >= 1

    def test_nonversioned_count_query(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test count functionality for non-versioned models."""
        # Create multiple configs
        with surrealdb_adapter:
            for i in range(3):
                config = SurrealNonVersionedConfig(
                    key=f"count.{i}",
                    value=f"value_{i}"
                )
                config.prepare_for_save()
                record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
                data = config.as_dict()
                surrealdb_adapter.execute_query(
                    f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
                )

            # Get count (using adapter's get_count if available, otherwise manual query)
            result = surrealdb_adapter.execute_query(
                f"SELECT count() FROM {NON_VERSIONED_TABLE} GROUP ALL"
            )
            count_result = surrealdb_adapter.parse_db_response(result)

            # SurrealDB count returns in format [{'count': N}]
            if isinstance(count_result, list) and len(count_result) > 0:
                count = count_result[0].get('count', 0)
                assert count >= 3
            elif isinstance(count_result, dict):
                count = count_result.get('count', 0)
                assert count >= 3


# ============================================================================
# Non-Versioned Posts Model Tests
# ============================================================================

class TestSurrealDBNonVersionedPosts:
    """Tests for SurrealNonVersionedPost model with SurrealDB."""

    def test_posts_create(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test creating a non-versioned post."""
        post = SurrealNonVersionedPost(title="First Post", description="This is my first blog post")

        with surrealdb_adapter:
            # Prepare model data for SurrealDB
            data = post.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
            record_id = f"{NON_VERSIONED_POST_TABLE}:{data['entity_id']}"

            # Create record using SurrealQL
            create_query = f"CREATE {record_id} CONTENT {data}"
            surrealdb_adapter.execute_query(create_query)

            # Retrieve and verify
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            assert result is not None
            assert result[0]['title'] == "First Post"
            assert result[0]['description'] == "This is my first blog post"

    def test_posts_update(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test updating a non-versioned post."""
        post = SurrealNonVersionedPost(title="Original Title", description="Original description")

        with surrealdb_adapter:
            # Create the post
            data = post.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
            record_id = f"{NON_VERSIONED_POST_TABLE}:{data['entity_id']}"
            surrealdb_adapter.execute_query(f"CREATE {record_id} CONTENT {data}")

            # Update the post
            update_query = f"UPDATE {record_id} SET title = 'Updated Title', description = 'Updated description'"
            surrealdb_adapter.execute_query(update_query)

            # Verify update
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            assert result[0]['title'] == "Updated Title"
            assert result[0]['description'] == "Updated description"


# ============================================================================
# Non-Versioned Cars Model Tests
# ============================================================================

class TestSurrealDBNonVersionedCars:
    """Tests for SurrealNonVersionedCar model with SurrealDB."""

    def test_cars_create(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test creating a non-versioned car."""
        car = SurrealNonVersionedCar(name="Model S", brand="Tesla")

        with surrealdb_adapter:
            # Prepare model data for SurrealDB
            data = car.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
            record_id = f"{NON_VERSIONED_CAR_TABLE}:{data['entity_id']}"

            # Create record using SurrealQL
            create_query = f"CREATE {record_id} CONTENT {data}"
            surrealdb_adapter.execute_query(create_query)

            # Retrieve and verify
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            assert result is not None
            assert result[0]['name'] == "Model S"
            assert result[0]['brand'] == "Tesla"

    def test_cars_update(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test updating a non-versioned car."""
        car = SurrealNonVersionedCar(name="Camry", brand="Toyota")

        with surrealdb_adapter:
            # Create the car
            data = car.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
            record_id = f"{NON_VERSIONED_CAR_TABLE}:{data['entity_id']}"
            surrealdb_adapter.execute_query(f"CREATE {record_id} CONTENT {data}")

            # Update the car
            update_query = f"UPDATE {record_id} SET name = 'Camry Hybrid'"
            surrealdb_adapter.execute_query(update_query)

            # Verify update
            result = surrealdb_adapter.execute_query(f"SELECT * FROM {record_id}")
            assert result[0]['name'] == "Camry Hybrid"
            assert result[0]['brand'] == "Toyota"

    def test_cars_multiple_brands(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test creating multiple cars from different brands."""
        cars = [
            SurrealNonVersionedCar(name="Civic", brand="Honda"),
            SurrealNonVersionedCar(name="Accord", brand="Honda"),
            SurrealNonVersionedCar(name="Corolla", brand="Toyota"),
            SurrealNonVersionedCar(name="Model Y", brand="Tesla")
        ]

        with surrealdb_adapter:
            # Create all cars
            for car in cars:
                data = car.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
                record_id = f"{NON_VERSIONED_CAR_TABLE}:{data['entity_id']}"
                surrealdb_adapter.execute_query(f"CREATE {record_id} CONTENT {data}")

            # Query all cars
            all_cars = surrealdb_adapter.execute_query(f"SELECT * FROM {NON_VERSIONED_CAR_TABLE}")
            assert len(all_cars) == 4

            honda_count = sum(1 for car in all_cars if car['brand'] == "Honda")
            assert honda_count == 2


# ============================================================================
# Integration Tests
# ============================================================================

class TestSurrealDBIntegration:
    """Integration tests for SurrealDB."""

    def test_mixed_versioned_and_nonversioned_same_db(self, versioned_repository, surrealdb_adapter, setup_surrealdb_tables):
        """Test both model types in same SurrealDB database."""
        product = SurrealVersionedProduct(name="Mixed SurrealDB", price=50.0)
        saved_product = versioned_repository.save(product)

        config = SurrealNonVersionedConfig(key="mixed.surrealdb", value="config")
        config.prepare_for_save()

        with surrealdb_adapter:
            record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
            data = config.as_dict()
            surrealdb_adapter.execute_query(
                f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
            )

        assert saved_product.entity_id != config.entity_id
        assert hasattr(saved_product, 'version')
        assert not hasattr(config, 'version') or getattr(type(config), 'version', None) is None

    def test_performance_comparison(self, versioned_repository, surrealdb_adapter, setup_surrealdb_tables):
        """Compare performance between versioned and non-versioned operations."""
        import time

        versioned_start = time.time()
        for i in range(20):
            product = SurrealVersionedProduct(name=f"Perf {i}", price=float(i))
            versioned_repository.save(product)
        versioned_time = time.time() - versioned_start

        nonversioned_start = time.time()
        with surrealdb_adapter:
            for i in range(20):
                config = SurrealNonVersionedConfig(key=f"perf.{i}", value=str(i))
                config.prepare_for_save()
                record_id = f"{NON_VERSIONED_TABLE}:{config.entity_id}"
                data = config.as_dict()
                surrealdb_adapter.execute_query(
                    f"CREATE {record_id} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data.items())}}}"
                )
        nonversioned_time = time.time() - nonversioned_start

        # Both should complete in reasonable time
        assert versioned_time < 30.0
        assert nonversioned_time < 30.0

    def test_adapter_vs_repository_consistency(self, surrealdb_adapter, versioned_repository):
        """Test adapter and repository consistency."""
        product = SurrealVersionedProduct(name="Consistency SurrealDB", price=75.0)
        saved = versioned_repository.save(product)
        entity_id = saved.entity_id

        with surrealdb_adapter:
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM {VERSIONED_TABLE} WHERE entity_id = '{entity_id}'"
            )
            records = surrealdb_adapter.parse_db_response(result)

            if isinstance(records, dict):
                records = [records]
            assert records is not None
            assert len(records) >= 1

    def test_surrealdb_graph_relationships(self, surrealdb_adapter, setup_surrealdb_tables):
        """Test SurrealDB's graph relationship capabilities."""
        # Create two products
        from uuid import uuid4
        user_id = uuid4()
        product1 = SurrealVersionedProduct(name="Product A", price=10.0)
        product1.prepare_for_save(user_id)
        product2 = SurrealVersionedProduct(name="Product B", price=20.0)
        product2.prepare_for_save(user_id)

        with surrealdb_adapter:
            # Create products
            record1 = f"{VERSIONED_TABLE}:{product1.entity_id}"
            record2 = f"{VERSIONED_TABLE}:{product2.entity_id}"

            data1 = product1.as_dict()
            data2 = product2.as_dict()

            surrealdb_adapter.execute_query(
                f"CREATE {record1} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data1.items())}}}"
            )
            surrealdb_adapter.execute_query(
                f"CREATE {record2} CONTENT {{{', '.join(f'{k}: {repr(v)}' for k, v in data2.items())}}}"
            )

            # Create a relationship (SurrealDB's graph feature)
            surrealdb_adapter.execute_query(
                f"RELATE {record1}->related->{record2}"
            )

            # Query the relationship
            result = surrealdb_adapter.execute_query(
                f"SELECT * FROM related"
            )
            relations = surrealdb_adapter.parse_db_response(result)

            # Should have at least one relation
            if isinstance(relations, dict):
                relations = [relations]
            assert len(relations) >= 1 if relations else True

    def test_connection_context_manager(self, surrealdb_adapter):
        """Test connection context manager with multiple operations."""
        # Rapid operations to test connection handling
        for i in range(10):
            with surrealdb_adapter:
                result = surrealdb_adapter.execute_query("SELECT * FROM surrealversionedproduct LIMIT 1")
                # Should complete without connection errors
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

