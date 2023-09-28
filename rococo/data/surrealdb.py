from surrealdb import Surreal
import asyncio


class SurrealDbAdapter():
    """SurrealDB adapter for interacting with SurrealDB."""

    def __init__(self, endpoint: str, username: str, password: str, namespace: str, db_name: str):
        """Initializes a new SurrealDB adapter."""
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self._namespace = namespace
        self._db_name = db_name
        self._db = None

    def __enter__(self):
        """Context manager entry point for preparing DB connection."""
        self._event_loop = asyncio.new_event_loop()
        self._db = self._event_loop.run_until_complete(self._prepare_db())
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point for closing DB connection."""
        self._event_loop.run_until_complete(self._db.close())
        self._event_loop.stop()
        self._event_loop = None
        self._db = None

    async def _prepare_db(self):
        """Prepares the DB connection."""
        db = Surreal(self._endpoint)
        await db.connect()
        await db.signin({"user": self._username, "pass": self._password})
        await db.use(self._namespace, self._db_name)
        return db
    
    def execute_query(self, sql, _vars=None):
        """Executes a query against the DB."""
        if _vars is None:
            _vars = {}

        if not self._db:
            raise Exception("No connection to SurrealDB.")

        return self._event_loop.run_until_complete(self._db.query(sql, _vars))
