# stdlib
import logging
import sys

# first party
from src.config import Config
from src.logging_config import setup_logging
from src.services.orchestrator import CiOrchestrator

# Set up logging before anything else
setup_logging()
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    try:
        # Create config from environment variables
        config = Config.from_env()

        # Create and run orchestrator
        orchestrator = CiOrchestrator(config)
        is_success = orchestrator.run()

        # Exit with appropriate status code
        sys.exit(0 if is_success else 1)
    except Exception:
        logger.exception("Fatal error in main process")
        sys.exit(1)
