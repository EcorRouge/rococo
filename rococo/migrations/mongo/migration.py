import logging
from pymongo.errors import OperationFailure
from rococo.migrations.common.migration_base import MigrationBase
from rococo.data.mongodb import MongoDBAdapter

# Name for the collection that will store migration metadata
METADATA_COLLECTION = 'rococo_migrations_meta'

class MongoMigration(MigrationBase):
    def __init__(self, db_adapter: MongoDBAdapter):
        super().__init__(db_adapter)
        self.db = self.db_adapter.db # Get the PyMongo Database object

    def _get_metadata_collection(self):
        return self.db[METADATA_COLLECTION]

    def ensure_metadata_collection(self):
        """Ensures the migration metadata collection exists."""
        collection = self._get_metadata_collection()
        if collection.count_documents({}) == 0:
            try:
                collection.insert_one({'_id': 'version_info', 'version': '0000000000'})
                logging.info(f"Initialized '{METADATA_COLLECTION}' with version 0000000000.")
            except OperationFailure as e:
                logging.error(f"Failed to initialize metadata collection: {e}")
                raise

    def get_current_db_version(self) -> str:
        """Gets the current DB version from the metadata collection."""
        self.ensure_metadata_collection()
        metadata_coll = self._get_metadata_collection()
        version_doc = metadata_coll.find_one({'_id': 'version_info'})
        if version_doc and 'version' in version_doc:
            return str(version_doc['version'])
        return '0000000000'

    def update_version_table(self, version: str, commit: bool = True):
        """Updates the DB version in the metadata collection."""
        self.ensure_metadata_collection()
        metadata_coll = self._get_metadata_collection()
        try:
            metadata_coll.update_one(
                {'_id': 'version_info'},
                {'$set': {'version': str(version)}},
                upsert=True
            )
            logging.info(f"Updated database version to: {version}")
        except OperationFailure as e:
            logging.error(f"Failed to update version to {version}: {e}")
            raise

    def create_collection(self, collection_name: str):
        """Creates a new collection."""
        try:
            self.db.create_collection(collection_name)
            logging.info(f"Collection '{collection_name}' created successfully.")
        except OperationFailure as e:
            if e.code == 48: # NamespaceExists
                 logging.warning(f"Collection '{collection_name}' already exists. Skipping creation.")
            else:
                logging.error(f"Failed to create collection '{collection_name}': {e}")
                raise

    def drop_collection(self, collection_name: str):
        """Drops a collection."""
        try:
            self.db.drop_collection(collection_name)
            logging.info(f"Collection '{collection_name}' dropped successfully.")
        except OperationFailure as e:
            logging.error(f"Failed to drop collection '{collection_name}': {e}")
            raise

    def create_index(self, collection_name: str, keys: list, index_options: dict = None):
        """Creates an index on a collection."""
        collection = self.db[collection_name]
        try:
            index_name = collection.create_index(keys, **(index_options or {}))
            logging.info(f"Index '{index_name}' created on collection '{collection_name}'.")
        except OperationFailure as e:
            logging.warning(f"Could not create index on '{collection_name}' (keys: {keys}). It might already exist or conflict: {e}")

    def drop_index(self, collection_name: str, index_name: str):
        """Drops an index from a collection by its name."""
        collection = self.db[collection_name]
        try:
            collection.drop_index(index_name)
            logging.info(f"Index '{index_name}' dropped from collection '{collection_name}'.")
        except OperationFailure as e:
            if e.code == 27: # IndexNotFound
                logging.warning(f"Index '{index_name}' not found on collection '{collection_name}'. Skipping drop.")
            else:
                logging.error(f"Failed to drop index '{index_name}' from '{collection_name}': {e}")
                raise

    def update_documents(self, collection_name: str, filter_query: dict, update_spec: dict, multi: bool = True):
        """Updates documents in a collection."""
        collection = self.db[collection_name]
        try:
            if multi:
                result = collection.update_many(filter_query, update_spec)
                logging.info(f"Updated {result.modified_count} documents in '{collection_name}'. Matched {result.matched_count}.")
            else:
                result = collection.update_one(filter_query, update_spec)
                logging.info(f"Updated {result.modified_count} document in '{collection_name}'. Matched {result.matched_count}.")
        except OperationFailure as e:
            logging.error(f"Failed to update documents in '{collection_name}': {e}")
            raise

    def execute(self, operation_callable, *args, **kwargs):
        """Executes a given callable operation."""
        with self.db_adapter:
            try:
                return operation_callable(*args, **kwargs)
            except Exception as e:
                logging.error(f"Error during MongoDB migration operation: {e}")
                raise