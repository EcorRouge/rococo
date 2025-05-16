"""
Tests for BaseRepository
"""
import json
import uuid
import pytest
from datetime import datetime

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel # Assuming this is the actual base class
from rococo.repositories.base_repository import BaseRepository
from rococo.messaging.base import MessageAdapter


# --- DummyVersionedModel ---
class DummyVersionedModel(VersionedModel):
    """
    Test Class for VersionedModel, enhanced for testing BaseRepository.
    It simulates the necessary interface of VersionedModel.
    """
    def __init__(self, entity_id=None, version=None, active=True, name=None, **kwargs):
        """
        Initialize a DummyVersionedModel object. This mimics the behavior of a real VersionedModel's
        __init__, but with fewer required parameters and no real validation.

        :param entity_id: The entity_id for the test object. Defaults to a random UUID.
        :param version: The version for the test object. Defaults to a random UUID.
        :param active: A boolean indicating whether the test object is active. Defaults to True.
        :param name: An example custom field for the model. Has no default.
        :param kwargs: Additional keyword arguments will be stored in the object's other_data attribute.
        """
        # Initialize attributes that from_dict would set, and as_dict would read/write.
        # These are also attributes the real VersionedModel would likely have.
        self.entity_id = entity_id if entity_id is not None else uuid.uuid4()
        self.version = version if version is not None else uuid.uuid4() # prepare_for_save typically handles versioning
        self.previous_version = kwargs.get("previous_version", uuid.UUID(int=0))
        self.active = active
        self.name = name # Example custom field for the model
        self.created_at = kwargs.get('created_at', datetime.utcnow())
        self.updated_at = kwargs.get('updated_at', datetime.utcnow())
        self.changed_by_id = kwargs.get('changed_by_id')
        self.other_data = {k:v for k,v in kwargs.items() if k not in ['created_at', 'updated_at', 'changed_by_id']}


    # prepare_for_save is a method on the actual VersionedModel.
    # BaseRepository calls instance.prepare_for_save().
    # We will mock this method on the *instance* in tests for save/delete
    # to verify it's called correctly and to control its side-effects if needed.
    # However, for this DummyVersionedModel to be complete if not mocked:
    def prepare_for_save(self, changed_by_id: uuid.UUID = None):
        """
        Prepares the model instance for saving by updating its metadata.

        This method sets the `created_at` timestamp if the instance is new,
        updates the `updated_at` timestamp, generates a new `version` UUID,
        and assigns the `changed_by_id` if provided. This ensures the model
        is ready for persistence with the latest metadata.

        Args:
            changed_by_id (uuid.UUID, optional): The ID of the user making the change.
        """
        if not hasattr(self, '_created_at_original') and not self.created_at: # Crude check for new
             self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.version = uuid.uuid4() # Always update version on save
        if changed_by_id:
            self.changed_by_id = changed_by_id

    @classmethod
    def from_dict(cls, data: dict) -> 'DummyVersionedModel':
        """
        Load DummyVersionedModel from dict.

        This method takes a dictionary and creates a DummyVersionedModel instance from it.
        It is used by BaseRepository to load data from the adapter.

        Args:
            data (dict): The data to load into the DummyVersionedModel instance.

        Returns:
            DummyVersionedModel: The loaded DummyVersionedModel instance.
        """
        if data is None:
            return None
        return cls(**data)

    def as_dict(self, convert_datetime_to_iso_string=False, convert_uuids=True):
        """
        Convert the DummyVersionedModel instance to a dictionary.

        This method generates a dictionary representation of the model, with options
        to convert UUIDs to strings and datetime objects to ISO formatted strings.

        Args:
            convert_uuids_to_string (bool, optional): Whether to convert UUID fields to strings.
            convert_datetime_to_iso_string (bool, optional): Whether to convert datetime fields to ISO strings.

        Returns:
            dict: A dictionary representation of the model instance, excluding None values.
        """

        data_dict = {
            "entity_id": str(self.entity_id) if convert_uuids and self.entity_id else self.entity_id,
            "version": str(self.version) if convert_uuids and self.version else self.version,
            "active": self.active,
            "name": self.name,
            "created_at": self.created_at.isoformat() if convert_datetime_to_iso_string and self.created_at else self.created_at,
            "updated_at": self.updated_at.isoformat() if convert_datetime_to_iso_string and self.updated_at else self.updated_at,
            "changed_by_id": str(self.changed_by_id) if convert_uuids and self.changed_by_id else self.changed_by_id,
        }
        if self.other_data:
            data_dict.update(self.other_data)
        
        # Simulate common practice of not sending None values for cleaner DB interaction
        return {k: v for k, v in data_dict.items() if v is not None}


# --- Fixtures ---
@pytest.fixture
def mock_db_adapter(mocker):
    """
    Mock for DbAdapter. Creates a mock DbAdapter, which is used to isolate the BaseRepository's
    interactions with the database. The mock adapter is set up to always return itself when
    entered as a context manager, and to immediately exit when exited as a context manager.
    Additionally, the adapter's parse_db_response method is set up to return its input unchanged
    unless overridden by the test. This allows tests to easily verify that the correct data was
    passed to parse_db_response, or to provide a specific return value for the method.
    """
    adapter = mocker.Mock(spec=DbAdapter)
    adapter.__enter__ = mocker.Mock(return_value=adapter)
    adapter.__exit__ = mocker.Mock()
    # Ensure parse_db_response returns its input by default if not overridden
    adapter.parse_db_response = mocker.Mock(side_effect=lambda x: x)
    return adapter

@pytest.fixture
def mock_message_adapter(mocker):
    """
    Mock for MessageAdapter. Creates a mock MessageAdapter, which is used to isolate the
    BaseRepository's interactions with the message bus. The mock adapter is set up to
    immediately return its send_message method when called, and to not be a context manager
    unless overridden by the test. This allows tests to easily verify that the correct message
    was sent, or to provide a specific return value for the method.
    """
    adapter = mocker.Mock(spec=MessageAdapter)
    # MessageAdapter doesn't seem to be a context manager in the provided BaseRepository code.
    # If it were, add __enter__ and __exit__ mocks.
    adapter.send_message = mocker.Mock()
    return adapter

@pytest.fixture
def test_user_id():
    """A sample UUID for user_id."""
    return uuid.uuid4()

@pytest.fixture
def repository(request, mock_db_adapter, mock_message_adapter, test_user_id, mocker):
    with_user_id = request.param
    user_id_to_use = test_user_id if with_user_id else None

    repo = BaseRepository(
        mock_db_adapter,
        DummyVersionedModel,
        mock_message_adapter,
        "test_queue_name",
        user_id=user_id_to_use
    )
    mocker.spy(repo, '_process_data_from_db')
    return repo



@pytest.fixture
def model_instance_mocker(mocker):
    """
    Creates a DummyVersionedModel instance and mocks its prepare_for_save and as_dict methods.
    This is useful for save/delete tests to precisely control these model interactions.
    """
    instance = DummyVersionedModel(name="Initial Name") # Create a real instance
    
    # Mock prepare_for_save on this specific instance
    # We use wraps to call the original DummyVersionedModel.prepare_for_save
    # while still being able to assert calls and arguments.
    # If DummyVersionedModel.prepare_for_save is too complex or relies on base VersionedModel
    # that we don't want to execute, we can remove `wraps`.
    mocker.patch.object(instance, 'prepare_for_save', wraps=instance.prepare_for_save)

    # Mock as_dict on this specific instance
    # We use wraps for similar reasons. If we want to control the exact dict returned,
    # we can set a `return_value` instead of `wraps`.
    mocker.patch.object(instance, 'as_dict', wraps=instance.as_dict)
    
    return instance

# --- Test Class for BaseRepository ---
class TestBaseRepository:
    """Test class for BaseRepository."""

    @pytest.mark.parametrize("repository", [True, False], indirect=True)
    def test_get_one_existing_record(self, repository: BaseRepository, mock_db_adapter, mocker):
        """
        Test retrieving one existing record from the repository.

        This test verifies that the `get_one` method retrieves an existing
        record by mocking the expected data and ensuring the correct calls
        are made to the database adapter. It also checks that the retrieved
        data is processed correctly and converted to a `DummyVersionedModel`
        instance.
        """
        expected_data = {'entity_id': uuid.uuid4(), 'name': 'Test Record', 'active': True}
        mock_db_adapter.get_one.return_value = expected_data
        
        # Spy on DummyVersionedModel.from_dict
        from_dict_spy = mocker.spy(DummyVersionedModel, 'from_dict')

        result = repository.get_one({'id': 1}, fetch_related=['details'])

        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.get_one.assert_called_once_with(
            repository.table_name,
            {'id': 1},
            fetch_related=['details']
        )
        repository._process_data_from_db.assert_called_once_with(expected_data)
        from_dict_spy.assert_called_once_with(expected_data)
        assert isinstance(result, DummyVersionedModel)
        assert result.name == 'Test Record'
        mock_db_adapter.__exit__.assert_called_once()

    def test_get_one_non_existing_record(self, repository: BaseRepository, mock_db_adapter, mocker):
        """
        Test retrieving a non-existing record from the repository.

        This test verifies that the `get_one` method correctly handles the case
        where no record is found. It ensures that the database adapter returns
        None, and that the method does not attempt to process or convert the
        non-existent data into a `DummyVersionedModel` instance. The test also
        confirms that the method returns None and that the appropriate calls are
        made to the database adapter.
        """
        mock_db_adapter.get_one.return_value = None
        from_dict_spy = mocker.spy(DummyVersionedModel, 'from_dict')

        result = repository.get_one({'id': 2})

        mock_db_adapter.get_one.assert_called_once_with(
            repository.table_name,
            {'id': 2},
            fetch_related=None # Default value
        )
        repository._process_data_from_db.assert_called_once_with(None)
        # DummyVersionedModel.from_dict might still be called with None if not handled inside get_one before calling
        # The current implementation of DummyVersionedModel.from_dict returns None if data is None.
        # Or, BaseRepository.get_one might return early.
        # Current BaseRepository code: `if not data: return None`. So from_dict won't be called.
        from_dict_spy.assert_not_called()
        assert result is None
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()

    def test_get_many_records(self, repository: BaseRepository, mock_db_adapter, mocker):
        """
        Test retrieving multiple records from the repository.

        This test verifies that the `get_many` method correctly handles the case
        where multiple records are found. It ensures that the database adapter
        returns a list of dictionaries, and that the method correctly processes
        this data into a list of `DummyVersionedModel` instances. The test also
        confirms that the method returns a list and that the appropriate calls are
        made to the database adapter.
        """
        record1_data = {'entity_id': uuid.uuid4(), 'name': 'Test1'}
        record2_data = {'entity_id': uuid.uuid4(), 'name': 'Test2'}
        db_response = [record1_data, record2_data]
        mock_db_adapter.get_many.return_value = db_response

        from_dict_spy = mocker.spy(DummyVersionedModel, 'from_dict')
        conditions = {'active': True}
        sort_order = [('name', 'asc')]
        limit = 50
        fetch_related_list = ['owner']

        result = repository.get_many(
            conditions=conditions,
            sort=sort_order,
            limit=limit,
            fetch_related=fetch_related_list
        )

        mock_db_adapter.get_many.assert_called_once_with(
            repository.table_name,
            conditions,
            sort_order,
            limit,
            fetch_related=fetch_related_list
        )
        repository._process_data_from_db.assert_called_once_with(db_response)
        assert from_dict_spy.call_count == 2
        from_dict_spy.assert_any_call(record1_data)
        from_dict_spy.assert_any_call(record2_data)

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], DummyVersionedModel)
        assert result[0].name == 'Test1'
        assert isinstance(result[1], DummyVersionedModel)
        assert result[1].name == 'Test2'
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()

    def test_get_many_single_dict_response(self, repository: BaseRepository, mock_db_adapter, mocker):
        """
        Test getting many records when a single dictionary is returned by the adapter.

        Test that the single dictionary is wrapped in a list by the BaseRepository and that
        the `from_dict` method is called once with the record data.
        """
        record_data = {'entity_id': uuid.uuid4(), 'name': 'Single Test'}
        mock_db_adapter.get_many.return_value = record_data # Single dict
        from_dict_spy = mocker.spy(DummyVersionedModel, 'from_dict')

        result = repository.get_many()

        repository._process_data_from_db.assert_called_once_with([record_data]) # BaseRepository wraps it
        from_dict_spy.assert_called_once_with(record_data)
        assert len(result) == 1
        assert isinstance(result[0], DummyVersionedModel)

    def test_get_many_empty_result(self, repository: BaseRepository, mock_db_adapter, mocker):
        """
        Test the `get_many` method when no records are returned.

        This test verifies that the `get_many` method correctly handles an empty
        result set from the database adapter. It ensures that no calls are made
        to the `from_dict` method, and that the processed data is an empty list.
        Additionally, it checks that the method returns an empty list and that
        the appropriate database adapter context methods are called.
        """
        mock_db_adapter.get_many.return_value = []
        from_dict_spy = mocker.spy(DummyVersionedModel, 'from_dict')

        result = repository.get_many()

        repository._process_data_from_db.assert_called_once_with([])
        from_dict_spy.assert_not_called()
        assert isinstance(result, list)
        assert len(result) == 0
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()

    def test_get_count(self, repository: BaseRepository, mock_db_adapter):
        """
        Test retrieving the count of records in a specified collection that match the given query parameters
        and index.

        Args:
            collection_name (str): The name of the collection to query.
            index (str): The name of the index to use for the query.
            query (Dict[str, Any]): Additional query parameters to filter the results.

        Returns:
            int: The count of matching records.
        """
        collection_name = "items"
        index_name = "item_name_idx"
        query_params = {"category": "electronics"}
        
        expected_db_conditions = {'active': True, "category": "electronics"}
        expected_adapter_options = {'hint': index_name}
        mock_db_adapter.get_count.return_value = 42

        count = repository.get_count(collection_name, index_name, query_params)

        assert count == 42
        mock_db_adapter.get_count.assert_called_once_with(
            collection_name,
            expected_db_conditions,
            options=expected_adapter_options
        )
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()

    def test_get_count_no_query_no_index(self, repository: BaseRepository, mock_db_adapter):
        """
        Test retrieving the count of records in a specified collection when no query parameters are
        specified and no index is provided.

        Args:
            collection_name (str): The name of the collection to query.

        Returns:
            int: The count of matching records.
        """
        collection_name = "users"
        expected_db_conditions = {'active': True} # Default condition
        mock_db_adapter.get_count.return_value = 100

        count = repository.get_count(collection_name, None, None) # No index, no query

        assert count == 100
        mock_db_adapter.get_count.assert_called_once_with(
            collection_name,
            expected_db_conditions,
            options=None # No options if no index
        )

    def test_save_new_instance(self, repository: BaseRepository, mock_db_adapter, model_instance_mocker, mocker):
        """
        Test saving a new instance to the database.

        This test verifies that the `save` method in the BaseRepository class correctly
        handles saving a new instance to the database. It tests that the instance is
        properly prepared for saving with the `prepare_for_save` method, and that the
        adapter's `get_save_query` and `run_transaction` methods are called correctly.
        The test also ensures that the method returns the same instance that was passed
        in as an argument, and that the appropriate database adapter context methods
        are called.
        """
        instance_to_save = model_instance_mocker # Use the instance with mocked methods
        original_entity_id = instance_to_save.entity_id # Capture before prepare_for_save
        
        # What as_dict (mocked on instance) should return after prepare_for_save
        # This depends on how DummyVersionedModel.as_dict and prepare_for_save are implemented/mocked
        # For this test, we assume model_instance_mocker.as_dict will be called and its return value used.
        # Let's define what data we expect it to return for the save query
        expected_data_for_save_query = {"name": "Updated Name", "active": True, "entity_id": str(instance_to_save.entity_id)}
        model_instance_mocker.as_dict.return_value = expected_data_for_save_query

        # Mock adapter query methods
        mock_move_query = ("MOVE_SQL", (original_entity_id,))
        mock_save_query = ("SAVE_SQL", ("Updated Name", True, original_entity_id))
        mock_db_adapter.get_move_entity_to_audit_table_query.return_value = mock_move_query
        mock_db_adapter.get_save_query.return_value = mock_save_query
        mock_db_adapter.run_transaction.return_value = True # Simulate successful transaction

        result = repository.save(instance_to_save, send_message=False)

        # 1. Verify _process_data_before_save behavior (prepare_for_save and as_dict calls)
        instance_to_save.prepare_for_save.assert_called_once_with(changed_by_id=repository.user_id)
        instance_to_save.as_dict.assert_called_once_with(convert_datetime_to_iso_string=True)
        
        # 2. Verify adapter calls
        mock_db_adapter.get_move_entity_to_audit_table_query.assert_called_once_with(
            repository.table_name,
            instance_to_save.entity_id # entity_id from the instance *after* prepare_for_save (if it changes it)
                                        # In our DummyVersionedModel, entity_id is set at init mostly.
        )
        mock_db_adapter.get_save_query.assert_called_once_with(
            repository.table_name,
            expected_data_for_save_query # This is crucial: data from the mocked as_dict
        )
        mock_db_adapter.run_transaction.assert_called_once_with([mock_move_query, mock_save_query])
        
        assert result is instance_to_save # Returns the same instance
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()


    def test_save_and_send_message(self, repository: BaseRepository, mock_db_adapter, mock_message_adapter, model_instance_mocker, mocker):
        """
        Tests saving an instance with the `save` method and sending a message.

        This test verifies that the `save` method in the BaseRepository class correctly
        handles saving an instance to the database and sending a message to the message queue.
        It tests that the instance is properly prepared for saving with the `prepare_for_save`
        method, and that the adapter's `get_save_query` and `run_transaction` methods are called
        correctly. It also ensures that the method returns the same instance that was passed
        in as an argument, and that the adapter's context methods are called.

        Additionally, it verifies that the message adapter's `send_message` method is called
        with the correct data, which is the result of calling `as_dict` on the instance after
        `prepare_for_save`. This data is expected to be the same as the data passed to the
        `get_save_query` method, but with the `updated_at` field converted to an ISO string.
        """
        instance_to_save = model_instance_mocker
        
        # Define what the instance's as_dict should return for the save query and for the message
        # For save query (after prepare_for_save, with convert_datetime_to_iso_string=True)
        data_for_save_query = {"entity_id": str(instance_to_save.entity_id), "name": "Test Save Message", "version": str(uuid.uuid4())}
        # For message (after prepare_for_save, with convert_datetime_to_iso_string=True)
        data_for_message = {"entity_id": str(instance_to_save.entity_id), "name": "Test Save Message", "version": data_for_save_query["version"], "updated_at": datetime.utcnow().isoformat()}

        # Configure the mocked as_dict on the instance
        # It will be called twice: once by _process_data_before_save, once for the message
        model_instance_mocker.as_dict.side_effect = [
            data_for_save_query, # First call for get_save_query
            data_for_message     # Second call for the message
        ]

        mock_db_adapter.get_move_entity_to_audit_table_query.return_value = ("AUDIT_SQL", ())
        mock_db_adapter.get_save_query.return_value = ("SAVE_SQL", ())
        mock_db_adapter.run_transaction.return_value = True

        result = repository.save(instance_to_save, send_message=True)

        assert result is instance_to_save
        instance_to_save.prepare_for_save.assert_called_once_with(changed_by_id=repository.user_id)
        
        # Check as_dict calls
        assert model_instance_mocker.as_dict.call_count == 2
        calls = model_instance_mocker.as_dict.call_args_list
        # First call (for DB save)
        assert calls[0] == mocker.call(convert_datetime_to_iso_string=True)
        # Second call (for message)
        assert calls[1] == mocker.call(convert_datetime_to_iso_string=True)


        mock_db_adapter.run_transaction.assert_called_once()
        mock_message_adapter.send_message.assert_called_once_with(
            repository.queue_name,
            json.dumps(data_for_message) # Data from the second call to as_dict
        )
        mock_db_adapter.__enter__.assert_called_once()
        mock_db_adapter.__exit__.assert_called_once()

    def test_delete(self, repository: BaseRepository, mock_db_adapter, model_instance_mocker, mocker):
        """
        Tests that the repository class correctly handles deleting an instance from the database.
        It verifies that the instance is properly prepared for deletion with the `prepare_for_delete`
        method, and that the adapter's `get_move_entity_to_audit_table_query` and `get_save_query`
        methods are called correctly. It also ensures that the method returns the same instance
        that was passed in as an argument, and that the adapter's context methods are called.

        It also verifies that the `prepare_for_save` method is called by `delete` on the instance,
        and that the `as_dict` method is called as well. The data passed to `get_save_query` is
        expected to be the same as the result of calling `as_dict` on the instance after
        `prepare_for_delete`. The `active` flag is set to `False` by the `prepare_for_delete`
        method.

        Calls to the adapter's `run_transaction` method are also verified.
        """
        instance_to_delete = model_instance_mocker
        instance_to_delete.active = True # Ensure it's active before deletion

        # Mock the internal save call's dependencies (prepare_for_save, as_dict)
        # When repository.delete calls repository.save, these instance methods will be hit.
        expected_data_after_prepare_for_delete = {"entity_id": str(instance_to_delete.entity_id), "active": False, "name": instance_to_delete.name}
        model_instance_mocker.as_dict.return_value = expected_data_after_prepare_for_delete


        # Mock adapter query methods for the internal save operation
        mock_move_query = ("MOVE_SQL_FOR_DELETE", ())
        mock_save_query = ("SAVE_SQL_FOR_DELETE", ())
        mock_db_adapter.get_move_entity_to_audit_table_query.return_value = mock_move_query
        mock_db_adapter.get_save_query.return_value = mock_save_query
        mock_db_adapter.run_transaction.return_value = True


        # Spy on the repository's save method to ensure it's called by delete
        # save_spy = mocker.spy(repository, 'save') # This won't work well if we also want to check internal calls of save

        result = repository.delete(instance_to_delete)

        assert result is instance_to_delete
        assert instance_to_delete.active is False # Verifies active flag is set

        # Verify that prepare_for_save was called (via the internal save call)
        # The instance passed to save inside delete() is `instance_to_delete` which already has active=False
        # prepare_for_save should still be called.
        instance_to_delete.prepare_for_save.assert_called_once_with(changed_by_id=repository.user_id)
        
        # Verify that as_dict was called (via the internal save call)
        instance_to_delete.as_dict.assert_called_once_with(convert_datetime_to_iso_string=True)

        # Verify adapter calls for the save operation within delete
        mock_db_adapter.get_move_entity_to_audit_table_query.assert_called_once_with(
            repository.table_name,
            instance_to_delete.entity_id
        )
        mock_db_adapter.get_save_query.assert_called_once_with(
            repository.table_name,
            expected_data_after_prepare_for_delete # Data from as_dict, reflecting active=False
        )
        mock_db_adapter.run_transaction.assert_called_once_with([mock_move_query, mock_save_query])
        
        mock_db_adapter.__enter__.assert_called_once() # Should be called by the save method
        mock_db_adapter.__exit__.assert_called_once()


# This allows running tests with `python test_file.py` if needed,
# but `pytest` is the standard runner.
if __name__ == '__main__':
    pytest.main([__file__])