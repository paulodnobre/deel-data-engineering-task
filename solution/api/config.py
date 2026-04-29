"""Configuration management for the Analytics API."""

import os
import logging
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Database Configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)

# Validate DATABASE_URL format on startup
def _validate_database_url(url: str) -> None:
    """Validate DATABASE_URL format on app startup."""
    try:
        parsed = urlparse(url)
        if parsed.scheme != "postgresql":
            raise ValueError(f"Invalid scheme: {parsed.scheme}. Expected 'postgresql'.")
        if not parsed.hostname:
            raise ValueError("Missing hostname in DATABASE_URL")
        logger.info(f"Database URL validated: postgresql://{parsed.hostname}:{parsed.port or 5432}/{parsed.path.lstrip('/')}")
    except Exception as e:
        logger.error(f"Invalid DATABASE_URL: {e}")
        raise

_validate_database_url(DATABASE_URL)

# Connection Pool Configuration
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", "4"))
MAX_POOL_SIZE = int(os.getenv("MAX_POOL_SIZE", "10"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logger.info(f"API configuration loaded: MIN_POOL={MIN_POOL_SIZE}, MAX_POOL={MAX_POOL_SIZE}, LOG_LEVEL={LOG_LEVEL}")
