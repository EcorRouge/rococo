from dbutils.pooled_db import PooledDB
import pymysql


class PooledConnectionPlugin:
    """
    A Flask plugin for managing pooled database connections using PooledDB.

    This plugin initializes a database connection pool and integrates it with
    a Flask application. It provides methods to retrieve pooled connections
    and ensures connections are properly closed after each request.
    """

    def __init__(self, app=None):
        """
        Initialize the plugin with optional Flask app and database configuration.
        
        :param app: Flask app instance
        :param db_config: Database configuration parameters (e.g., host, user, password)
        """
        self.pool = None
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """
        Initialize the Flask app with the PooledDB instance.
        
        :param app: Flask app instance
        """

        # Initialize the connection pool
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=app.config.get('MYSQL_POOL_MAX_CONNECTIONS'),
            host=app.config.get('MYSQL_HOST'),
            port=app.config.get('MYSQL_PORT'),
            user=app.config.get('MYSQL_USER'),
            password=app.config.get('MYSQL_PASSWORD'),
            database=app.config.get('MYSQL_DATABASE'),
            cursorclass=app.config.get('MYSQL_CURSORCLASS', pymysql.cursors.DictCursor)
        )

         # Store the plugin in Flask app extensions
        app.extensions['pooled_db'] = self

        # Register teardown function
        app.teardown_appcontext(self._teardown)

    def get_connection(self, *args, **kwargs):
        """
        Get a database connection from the pool or flask.g object if cached.
        
        :return: Pooled database connection
        """
        from flask import g

        if getattr(g, 'db_conn', None):
            return g.db_conn
        else:
            if not self.pool:
                raise RuntimeError("Database pool is not initialized. Call init_app() first.")
            db_conn = self.pool.connection()
            g.db_conn = db_conn
            return g.db_conn

    def _teardown(self, exception):
        """
        Teardown function to close database connections (if any) after a request.
        
        :param exception: Any exception raised during the request
        """
        from flask import g

        db_conn = g.pop('db_conn', None)
        if db_conn:
            db_conn.close()  # Return the connection to the pool
