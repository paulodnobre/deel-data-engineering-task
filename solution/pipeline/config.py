"""Configuration management for the analytics pipeline.

All database URLs and pipeline settings configured via environment.
"""
import os
import logging
from dotenv import load_dotenv

# Load .env file at module import time
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    """Configuration class for pipeline environment variables.

    Provides sensible defaults for all settings; overrideable via environment variables.
    No hardcoded credentials.
    """

    # Source database connection URL
    POSTGRES_SOURCE_URL = os.getenv(
        'POSTGRES_SOURCE_URL',
        'postgresql://postgres:password@localhost:5432/deel'
    )

    # Analytics database connection URL
    POSTGRES_ANALYTICS_URL = os.getenv(
        'POSTGRES_ANALYTICS_URL',
        'postgresql://postgres:password@localhost:5432/deel'
    )

    # Batch size for bulk inserts (tunable for performance)
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))

    # Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # Connection pool sizes (min_connections, max_connections)
    DB_POOL_MIN_CONNECTIONS = int(os.getenv('DB_POOL_MIN_CONNECTIONS', '1'))
    DB_POOL_MAX_CONNECTIONS = int(os.getenv('DB_POOL_MAX_CONNECTIONS', '5'))

    @classmethod
    def validate(cls):
        """Validate configuration at startup.

        Ensures critical env vars are set and have sensible values.
        """
        errors = []

        if not cls.POSTGRES_SOURCE_URL:
            errors.append("POSTGRES_SOURCE_URL not configured")
        if not cls.POSTGRES_ANALYTICS_URL:
            errors.append("POSTGRES_ANALYTICS_URL not configured")
        if cls.BATCH_SIZE <= 0:
            errors.append("BATCH_SIZE must be positive")
        if cls.DB_POOL_MAX_CONNECTIONS < cls.DB_POOL_MIN_CONNECTIONS:
            errors.append("DB_POOL_MAX_CONNECTIONS must be >= DB_POOL_MIN_CONNECTIONS")

        if errors:
            logger.error("Configuration validation failed: " + "; ".join(errors))
            return False

        return True
