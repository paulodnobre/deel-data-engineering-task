"""Orchestrator for analytics pipeline.

Supports flexible pipeline execution modes:
- PIPELINE_MODE=backfill-only: Run backfill job and exit
- PIPELINE_MODE=stream-only: Skip backfill, run consumer only
- PIPELINE_MODE=backfill-then-stream: Run backfill, then start consumer (default)

Flow:
1. Parse PIPELINE_MODE from environment (default: 'backfill-then-stream')
2. Set up logging via logging_config.setup_logging()
3. Instantiate BackfillJob and AnalyticsConsumer
4. Execute based on PIPELINE_MODE
5. Handle graceful shutdown on SIGINT/SIGTERM
"""
import os
import sys
import logging
import signal
from datetime import datetime

from pipeline.config import Config
from pipeline.backfill import BackfillJob
from pipeline.consumer import AnalyticsConsumer
from pipeline.logging_config import setup_logging, get_logger

# Global flags for graceful shutdown
shutdown_requested = False
logger = None


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown.

    Handles SIGINT (Ctrl+C) and SIGTERM for clean shutdown.
    Sets shutdown_requested flag to stop backfill and consumer cleanly.
    """
    def signal_handler(signum, frame):
        global shutdown_requested
        shutdown_requested = True
        logger.warning("Shutdown signal received (sig=%d); initiating graceful shutdown...", signum)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main orchestrator function.

    Parses PIPELINE_MODE environment variable and executes the appropriate pipeline:
    - backfill-only: Run backfill job only, exit when complete
    - stream-only: Skip backfill, run consumer indefinitely
    - backfill-then-stream: Run backfill first, then start consumer (default)

    Raises SystemExit on fatal errors (invalid PIPELINE_MODE, config validation failure)
    """
    global logger

    # Validate configuration at startup
    if not Config.validate():
        print("Configuration validation failed; exiting", file=sys.stderr)
        sys.exit(1)

    # Set up logging based on Config.LOG_LEVEL
    logger = setup_logging(Config.LOG_LEVEL)
    logger_main = get_logger(__name__)

    # Set up signal handlers for graceful shutdown
    setup_signal_handlers()

    # Parse PIPELINE_MODE from environment (default: backfill-then-stream)
    pipeline_mode = os.getenv('PIPELINE_MODE', 'backfill-then-stream').lower()

    logger_main.info("Pipeline started in mode: %s", pipeline_mode)

    # Instantiate backfill and consumer jobs
    backfill_job = BackfillJob(
        source_url=Config.POSTGRES_SOURCE_URL,
        analytics_url=Config.POSTGRES_ANALYTICS_URL,
        batch_size=Config.BATCH_SIZE
    )

    # For consumer, we need Kafka brokers; use env var or parse from Config
    kafka_brokers_str = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    kafka_brokers = [b.strip() for b in kafka_brokers_str.split(',')]

    consumer_job = AnalyticsConsumer(
        kafka_brokers=kafka_brokers,
        analytics_url=Config.POSTGRES_ANALYTICS_URL
    )

    try:
        # Execute based on PIPELINE_MODE
        if pipeline_mode == 'backfill-only':
            logger_main.info("Executing backfill-only mode")
            # Run backfill first with ON CONFLICT upsert for idempotency — allows re-runs without duplication
            backfill_job.run()
            logger_main.info("Backfill complete; exiting")
            sys.exit(0)

        elif pipeline_mode == 'backfill-then-stream':
            logger_main.info("Executing backfill-then-stream mode")
            logger_main.info("Starting backfill...")
            # Run backfill first to populate analytics schema before streaming updates from Kafka
            backfill_job.run()
            logger_main.info("Backfill complete; starting consumer...")
            consumer_job.run()

        elif pipeline_mode == 'stream-only':
            logger_main.info("Executing stream-only mode; skipping backfill")
            consumer_job.run()

        else:
            logger_main.error("Unknown PIPELINE_MODE: %s", pipeline_mode)
            logger_main.info("Valid modes: backfill-only, stream-only, backfill-then-stream")
            sys.exit(1)

    except KeyboardInterrupt:
        logger_main.warning("Shutdown initiated by user (Ctrl+C)")
        sys.exit(0)

    except Exception as exc:
        logger_main.error("Fatal error in pipeline: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
