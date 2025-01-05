# stdlib
import logging
import sys

# first party
from src.config import Config
from src.services.orchestrator import CiOrchestrator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    # Create config from environment variables
    config = Config.from_env()

    # Create and run orchestrator
    orchestrator = CiOrchestrator(config)
    success = orchestrator.run()

    # Exit with appropriate status code
    sys.exit(0 if success else 1)
