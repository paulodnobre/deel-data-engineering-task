"""Centralized logging configuration for analytics pipeline.

Provides:
- setup_logging(level='INFO'): Initializes logger with StreamHandler + FileHandler
- get_logger(name): Returns configured logger for per-module use

"""
import logging
import logging.handlers
import os


def setup_logging(level='INFO'):
    """Set up centralized logging for pipeline package.

    Configures root logger for 'pipeline' package with:
    - StreamHandler: stdout with formatted output
    - FileHandler: logs/pipeline.log (optional, if directory exists)
    - Format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
               Default: INFO

    Returns:
        logging.Logger: Configured logger for 'pipeline' package

    Examples:
        >>> setup_logging('DEBUG')  # Enable debug-level logs for development
        >>> setup_logging('ERROR')  # Errors only for production
    """
    # Create logger for pipeline package
    logger = logging.getLogger('pipeline')
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Log format: timestamp, level, logger name, message
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # StreamHandler: output to console (stdout)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # FileHandler: output to logs/pipeline.log (if directory exists)
    logs_dir = 'logs'
    if os.path.isdir(logs_dir):
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(logs_dir, 'pipeline.log'),
            maxBytes=10485760,  # 10 MB
            backupCount=5       # Keep 5 backups
        )
        file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name):
    """Get logger for a specific module within pipeline package.

    Returns a logger that inherits configuration from the 'pipeline'
    package root logger (set up via setup_logging()).

    Logger names are hierarchical:
    - 'pipeline.backfill' for backfill.py
    - 'pipeline.consumer' for consumer.py
    - 'pipeline.db' for db.py

    Args:
        name: Module name (typically __name__)

    Returns:
        logging.Logger: Logger for the module with inherited config

    Examples:
        >>> logger = get_logger('pipeline.consumer')
        >>> logger.info("Consumer started")
        >>> logger.debug("Offset tracked")
        >>> logger.error("Failed to write")
    """
    return logging.getLogger(name)
