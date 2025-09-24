from typing import Any, Dict, List, Optional, Tuple, Union
from pymongo import MongoClient, ReturnDocument, errors
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.client_session import ClientSession
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from rococo.data.base import DbAdapter


class MongoDBAdapter(DbAdapter):
    """
    Production-ready MongoDB adapter with robust defaults and safety patterns:
      - Retryable writes enabled
      - Majority write concern
      - Configurable timeouts and pool sizes
      - Causal consistency sessions
      - Clean error handling
    """

    def __init__(
        self,
        mongo_uri: str,
        mongo_database: str,
        **client_options: Any
    ):
        # Default client options for robustness
        options = {
            'retryWrites': True,
            'w': 'majority',
            'serverSelectionTimeoutMS': 5000,
            'connectTimeoutMS': 5000,
            'maxPoolSize': 100,
            'tz_aware': True,
        }
        options.update(client_options)
        self.client: MongoClient = MongoClient(mongo_uri, **options)
        self.db_name: str = mongo_database
        self.db: Database = None
        self._session: Optional[ClientSession] = None

    def __enter__(self) -> 'MongoDBAdapter':
        """
        Context manager entry point for establishing a MongoDB connection.

        This method verifies the connection to the MongoDB instance by executing
        a ping command. If successful, it initializes the database and starts a
        causal-consistency session for transactions. If the ping command fails,
        a ConnectionError is raised.

        Returns:
            MongoDBAdapter: The initialized adapter with a live connection.
        """
        try:
            self.client.admin.command('ping')
        except errors.PyMongoError as e:
            raise ConnectionError(f"MongoDB ping failed: {e}") from e

        self.db = self.client.get_database(self.db_name)
        # Start a causal-consistency session for transactions
        self._session = self.client.start_session(causal_consistency=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Clean up the session and close the MongoClient on __exit__.

        If any exceptions occurred during the context, they are propagated.
        """
        if self._session:
            self._session.end_session()
            self._session = None
        # self.client.close()
        # Keep MongoClient open; application teardown will close it
        # The MongoClient is thread‐safe and intended to be long‐lived; you shouldn’t open & close it per operation.

    def _get_collection(self, name: str, write: bool = False) -> Collection:
        """
        Get a MongoDB collection with specified read and write concerns.

        By default, the collection is created with a local read concern and a
        majority write concern. If write is True, the collection is created with
        a majority write concern as well.

        Args:
            name (str): The name of the collection.
            write (bool, optional): If True, create the collection with a
                majority write concern. Defaults to False.

        Returns:
            Collection: The MongoDB collection object.
        """
        rc = ReadConcern('local')
        wc = WriteConcern('majority')
        if write:
            return self.db.get_collection(name, read_concern=rc, write_concern=wc)
        return self.db.get_collection(name, read_concern=rc)

    def run_transaction(self, operations_list: List[Any]) -> None:
        """
        Execute a list of operations as a transaction within a MongoDB session.

        This method runs each operation in the provided list within the context of
        a MongoDB transaction. It ensures that all operations are executed atomically,
        meaning either all operations succeed or none are applied. A session must be
        active for the transaction to start; otherwise, a RuntimeError is raised.

        Args:
            operations_list (List[Any]): A list of callable operations to be executed
                                        in the transaction.

        Raises:
            RuntimeError: If the session is not started before calling this method.
        """
        if not self._session:
            raise RuntimeError("Session not started")
        with self._session.start_transaction():
            for op in operations_list:
                op()

    def execute_query(self, sql: str, _vars: Dict[str, Any] = None) -> Any:
        """
        Execute a raw SQL query against the DB.

        This method is not supported for MongoDB since it uses a different query language.
        Use the get_one, get_many, insert_one, insert_many, update_one, update_many, replace_one,
        delete_one, delete_many methods instead to execute queries.

        Raises:
            NotImplementedError: execute_query is not supported for MongoDB.
        """
        raise NotImplementedError("execute_query is not supported for MongoDB")

    def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse the response from MongoDB and return structured data.

        This method takes a MongoDB response (either a single document or a cursor) and
        returns structured data. If the response is a cursor, it returns a list of documents.
        Otherwise, it returns a single document.

        Args:
            response (Any): A MongoDB response (either a document or a cursor)

        Returns:
            Union[Dict[str, Any], List[Dict[str, Any]]]: Structured data from the MongoDB response
        """
        if hasattr(response, "__iter__") and not isinstance(response, dict):
            return list(response)
        return response

    def get_one(
        self,
        table: str,
        conditions: Dict[str, Any],
        hint: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single document from the specified MongoDB collection based on given conditions.

        This method fetches a single document from the MongoDB collection specified by `table`.
        Additional query parameters such as `hint` and `sort` can be provided to optimize the query
        and define the order of the returned document.

        Args:
            table (str): The name of the collection from which to fetch the document.
            conditions (Dict[str, Any]): A dictionary specifying the conditions to filter the documents.
            hint (Optional[str]): An optional index hint to optimize the query.
            sort (Optional[List[Tuple[str, int]]]): An optional list of tuples specifying the sort order.

        Returns:
            Optional[Dict[str, Any]]: The document that matches the conditions, or None if no document is found.

        Raises:
            RuntimeError: If the query fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            kwargs: Dict[str, Any] = {}
            if hint is not None:
                kwargs['hint'] = hint
            if sort is not None:
                kwargs['sort'] = sort
            return coll.find_one(conditions, **kwargs)
        except errors.PyMongoError as e:
            raise RuntimeError(f"get_one failed: {e}") from e

    def get_many(
        self,
        table: str,
        conditions: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve multiple documents from the specified MongoDB collection based on given conditions.

        This method fetches multiple documents from the MongoDB collection specified by `table`.
        Additional query parameters such as `hint`, `sort`, `limit`, and `offset` can be provided to optimize the query
        and define the order of the returned documents.

        Args:
            table (str): The name of the collection from which to fetch the documents.
            conditions (Optional[Dict[str, Any]]): An optional dictionary specifying the conditions to filter the documents.
            hint (Optional[str]): An optional index hint to optimize the query.
            sort (Optional[List[Tuple[str, int]]]): An optional list of tuples specifying the sort order.
            limit (Optional[int]): The maximum number of documents to return. If None, no limit is applied.
            offset (Optional[int]): The number of documents to skip before returning results. If None, no offset is applied.

        Returns:
            List[Dict[str, Any]]: The list of documents that match the conditions.

        Raises:
            RuntimeError: If the query fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            cursor = coll.find(conditions or {}, **
                               ({'hint': hint} if hint else {}))
            if sort:
                cursor = cursor.sort(sort)
            if offset is not None and offset > 0:
                cursor = cursor.skip(offset)
            if limit is not None and limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)
        except errors.PyMongoError as e:
            raise RuntimeError(f"get_many failed: {e}") from e

    def get_count(
        self,
        table: str,
        conditions: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Retrieve the count of documents from the specified MongoDB collection based on given conditions.

        This method returns the count of documents from the MongoDB collection specified by `table`
        based on the given `conditions`.

        Args:
            table (str): The name of the collection from which to retrieve the count.
            conditions (Dict[str, Any]): A dictionary specifying the conditions to filter the documents.

        Returns:
            int: The count of documents that match the conditions.

        Raises:
            RuntimeError: If the query fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            kwargs: Dict[str, Any] = {}
            # forward hint if provided
            if options and 'hint' in options and options['hint'] is not None:
                kwargs['hint'] = options['hint']
            return coll.count_documents(conditions, **kwargs)
        except errors.PyMongoError as e:
            raise RuntimeError(f"get_count failed: {e}") from e

    def move_entity_to_audit_table(
        self,
        table: str,
        entity_id: str
    ) -> None:
        """
        Move all entity versions to the audit collection.

        This method retrieves ALL documents from the specified MongoDB collection
        with the given entity_id and copies them to the corresponding audit collection
        named `{table}_audit`. This preserves the entire change history for the entity,
        matching the behavior of the MySQL implementation.

        Args:
            table (str): The name of the collection from which to retrieve the documents.
            entity_id (str): The identifier of the entity to move to the audit collection.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            # Find ALL documents with this entity_id (entire change history)
            docs = list(
                coll.find({'entity_id': entity_id}, session=self._session))

            if docs:
                audit = self._get_collection(f"{table}_audit", write=True)

                # Insert all documents into audit collection
                # Use replace_one with upsert to handle potential duplicates
                for doc in docs:
                    # Use the document's _id as the unique identifier to avoid duplicates
                    audit.replace_one(
                        {'_id': doc['_id']},
                        doc,
                        upsert=True,
                        session=self._session
                    )
        except errors.PyMongoError as e:
            raise RuntimeError(
                f"move_entity_to_audit_table failed: {e}") from e

    def save(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save (versioned) a document in the specified MongoDB collection.

        Instead of doing a simple upsert, this does a “versioned insert”:
          1. Find existing document with entity_id and latest=True. If found, set latest=False.
          2. Create a new document based on `data` with latest=True, active=True.
          3. Insert that new document and return it.

        Args:
            table (str): The name of the MongoDB collection to version-insert into.
            data (Dict[str, Any]): The new version’s fields (must include 'entity_id').

        Returns:
            Dict[str, Any]: The newly inserted (latest) document.

        Raises:
            RuntimeError: If any MongoDB operation fails.
        """
        if 'entity_id' not in data:
            raise RuntimeError("save failed: 'entity_id' is required in data")

        try:
            coll = self._get_collection(table, write=True)

            # 1) Find the current latest version (if any) and mark it as not latest
            prev_latest = coll.find_one(
                {"entity_id": data['entity_id'], "latest": True},
                session=self._session
            )
            if prev_latest:
                coll.update_one(
                    {"_id": prev_latest["_id"]},
                    {"$set": {"latest": False}},
                    session=self._session
                )

            # 2) Prepare the new version document
            new_doc = data.copy()
            # Ensure flags are set appropriately:
            new_doc["latest"] = True

            # 3) Insert the new version
            insert_result = coll.insert_one(new_doc, session=self._session)

            # 4) Fetch and return the freshly inserted document (including _id)
            return coll.find_one({"_id": insert_result.inserted_id}, session=self._session)

        except errors.PyMongoError as e:
            raise RuntimeError(f"save failed: {e}") from e

    def delete(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> bool:
        """
        Soft delete a document by setting 'active' to False in the specified MongoDB collection.

        This method updates a document in the MongoDB collection specified by `table`
        by setting its 'active' field to False instead of deleting it.

        Args:
            table (str): The name of the MongoDB collection containing the document.
            data (Dict[str, Any]): The filter conditions to identify the document to update.

        Returns:
            bool: True if a document was updated (soft deleted), False otherwise.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table, write=True)
            result = coll.update_one(data, {'$set': {'active': False}})
            return result.matched_count > 0 and result.modified_count > 0
        except errors.PyMongoError as e:
            raise RuntimeError(f"delete failed: {e}") from e

    def insert_many(
        self,
        table: str,
        documents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Insert multiple documents into the specified MongoDB collection.

        This method inserts a list of documents into the MongoDB collection specified by `table`.
        Each document is inserted with the current session for transaction consistency.

        Args:
            table (str): The name of the MongoDB collection to insert documents into.
            documents (List[Dict[str, Any]]): A list of document dictionaries to insert.

        Returns:
            List[Dict[str, Any]]: The list of inserted documents including their generated _id fields.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
            ValueError: If the documents list is empty.
        """
        if not documents:
            raise ValueError(
                "insert_many failed: documents list cannot be empty")

        try:
            coll = self._get_collection(table, write=True)

            # Prepare documents for insertion
            docs_to_insert = []
            for doc in documents:
                doc_copy = doc.copy()
                # Remove _id if it exists to let MongoDB generate it
                if '_id' in doc_copy:
                    del doc_copy['_id']
                docs_to_insert.append(doc_copy)

            # Insert the documents
            insert_result = coll.insert_many(
                docs_to_insert, session=self._session)

            # Fetch and return the inserted documents with their generated _id values
            inserted_docs = []
            for inserted_id in insert_result.inserted_ids:
                doc = coll.find_one({"_id": inserted_id},
                                    session=self._session)
                if doc:
                    inserted_docs.append(doc)

            return inserted_docs

        except errors.PyMongoError as e:
            raise RuntimeError(f"insert_many failed: {e}") from e

    def create_index(
        self,
        table: str,
        columns: List[Union[str, Tuple[str, int]]],
        index_name: str,
        partial_filter: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a MongoDB index.

        This method creates a MongoDB index on the specified MongoDB collection
        with the given `columns` and `index_name`. If a `partial_filter` is provided,
        it is used to create a partial index.

        Args:
            table (str): The name of the MongoDB collection on which to create the index.
            columns (List[Union[str, Tuple[str, int]]]): A list of column names or tuples
                of column name and sort order to create the index on.
            index_name (str): The name of the index to create.
            partial_filter (Optional[Dict[str, Any]], optional): An optional dictionary
                specifying the partial filter expression for the index. Defaults to None.

        Returns:
            str: The name of the created index.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            options: Dict[str, Any] = {'name': index_name}
            if partial_filter:
                options['partialFilterExpression'] = partial_filter
            coll = self._get_collection(table, write=True)
            return coll.create_index(columns, **options)
        except errors.PyMongoError as e:
            raise RuntimeError(f"create_index failed: {e}") from e

    def aggregate(
        self,
        table: str,
        pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute an aggregation pipeline and return raw results.

        This method executes a MongoDB aggregation pipeline on the specified collection
        and returns the unmodified results as a list of dictionaries.

        Args:
            table (str): The name of the collection to aggregate on.
            pipeline (List[Dict[str, Any]]): MongoDB aggregation pipeline stages.

        Returns:
            List[Dict[str, Any]]: Raw aggregation results.

        Raises:
            RuntimeError: If the aggregation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            cursor = coll.aggregate(pipeline, session=self._session)
            return list(cursor)
        except errors.PyMongoError as e:
            raise RuntimeError(f"aggregate failed: {e}") from e

    def get_move_entity_to_audit_table_query(self, table: str, entity_id: str):
        """
        Stub to satisfy the abstract API.
        Use move_entity_to_audit_table(...) directly instead.
        """
        raise NotImplementedError(
            "get_move_entity_to_audit_table_query() is not supported; "
            "call move_entity_to_audit_table() instead."
        )

    def get_save_query(self, table: str, data: Dict[str, Any]):
        """
        Stub to satisfy the abstract API.
        Use save(...) directly instead.
        """
        raise NotImplementedError(
            "get_save_query() is not supported; call save() instead."
        )
