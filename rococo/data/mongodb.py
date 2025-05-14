from rococo.data.base import DbAdapter
from pymongo.collection import Collection
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.client_session import ClientSession
from pymongo import MongoClient, ReturnDocument, errors
from typing import Any, Dict, List, Optional, Tuple, Union


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
        }
        options.update(client_options)
        self.client = MongoClient(mongo_uri, **options)
        self.db_name = mongo_database
        self.db: Any = None
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
            # Verify connection
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
        self.client.close()

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
            raise RuntimeError('Session not started')
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
        raise NotImplementedError('execute_query is not supported for MongoDB')

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

        if hasattr(response, '__iter__') and not isinstance(response, dict):
            return list(response)
        return response

    def get_one(
        self,
        table: str,
        conditions: Dict[str, Any],
        hint: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
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
            if hint:
                kwargs['hint'] = hint
            if sort:
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
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve multiple documents from the specified MongoDB collection based on given conditions.

        This method fetches multiple documents from the MongoDB collection specified by `table`.
        Additional query parameters such as `hint`, `sort`, and `limit` can be provided to optimize the query
        and define the order of the returned documents.

        Args:
            table (str): The name of the collection from which to fetch the documents.
            conditions (Optional[Dict[str, Any]]): An optional dictionary specifying the conditions to filter the documents.
            hint (Optional[str]): An optional index hint to optimize the query.
            sort (Optional[List[Tuple[str, int]]]): An optional list of tuples specifying the sort order.
            limit (int): The maximum number of documents to return. Defaults to 100.

        Returns:
            List[Dict[str, Any]]: The list of documents that match the conditions.

        Raises:
            RuntimeError: If the query fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            if hint:
                cursor = coll.find(conditions or {}, hint=hint)
            else:
                cursor = coll.find(conditions or {})
            if sort:
                cursor = cursor.sort(sort)
            if limit and limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)
        except errors.PyMongoError as e:
            raise RuntimeError(f"get_many failed: {e}") from e

    def get_count(self, table: str, conditions: Dict[str, Any]) -> int:
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
            return coll.count_documents(conditions)
        except errors.PyMongoError as e:
            raise RuntimeError(f"get_count failed: {e}") from e

    def move_entity_to_audit_table(
        self,
        table: str,
        entity_id: str
    ) -> None:
        """
        Move an entity to the audit collection.

        This method retrieves a document from the specified MongoDB collection
        using the given entity_id and inserts it into a corresponding audit collection
        named `{table}_audit`.

        Args:
            table (str): The name of the collection from which to retrieve the document.
            entity_id (str): The identifier of the entity to move to the audit collection.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table)
            doc = coll.find_one({'entity_id': entity_id})
            if doc:
                audit = self._get_collection(f"{table}_audit", write=True)
                audit.insert_one(doc)
        except errors.PyMongoError as e:
            raise RuntimeError(f"move_entity_to_audit_table failed: {e}") from e

    def save(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save or update a document in the specified MongoDB collection.

        This method attempts to update an existing document with the provided data 
        based on the `entity_id`. If no document matches the `entity_id`, a new 
        document is inserted.

        Args:
            table (str): The name of the MongoDB collection to update or insert the document.
            data (Dict[str, Any]): The data to be saved or updated in the collection.

        Returns:
            Dict[str, Any]: The updated or newly inserted document.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table, write=True)
            return coll.find_one_and_update(
                {'entity_id': data['entity_id']},
                {'$set': data},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except errors.PyMongoError as e:
            raise RuntimeError(f"save failed: {e}") from e

    def delete(
        self,
        table: str,
        conditions: Dict[str, Any]
    ) -> bool:
        """
        Delete a document from the specified MongoDB collection based on given conditions.

        This method deletes a document from the MongoDB collection specified by `table`
        based on the given `conditions`.

        Args:
            table (str): The name of the MongoDB collection from which to delete the document.
            conditions (Dict[str, Any]): The conditions to filter the documents to be deleted.

        Returns:
            bool: True if a document was deleted, False otherwise.

        Raises:
            RuntimeError: If the operation fails due to a PyMongoError.
        """
        try:
            coll = self._get_collection(table, write=True)
            result = coll.delete_one(conditions)
            return result.deleted_count > 0
        except errors.PyMongoError as e:
            raise RuntimeError(f"delete failed: {e}") from e

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
