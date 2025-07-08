"""data module"""

from .base import DbAdapter
import logging

logger = logging.getLogger(__name__)


# Conditional imports - only import if dependencies are available
try:
    from .mongodb import MongoDBAdapter
except ImportError:
    logger.info("MongoDBAdapter not loaded - probably, missing dependencies")
    pass

try:
    from .mysql import MySqlAdapter
except ImportError:
    logger.info("MySqlAdapter not loaded - probably, missing dependencies")
    pass

try:
    from .postgresql import PostgreSQLAdapter
except ImportError:
    logger.info("PostgreSQLAdapter not loaded - probably, missing dependencies")
    pass

try:
    from .surrealdb import SurrealDbAdapter
except ImportError:
    logger.info("SurrealDbAdapter not loaded - probably, missing dependencies")
    pass
