# stdlib
import logging
import subprocess
import sys

# first party
from src.config import Config
from src.dbt import (
    get_all_unique_ids,
    get_source_compiled_code,
    get_target_compiled_code,
)
from src.node import NodeManager
from src.utils import JobRunStatus, create_dbt_cloud_profile, trigger_job

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DBT_COMMANDS: dict[str, list[str]] = {
    "compile": [
        "dbt",
        "compile",
        "-s",
        "state:modified,resource_type:model",
        "--favor-state",
    ],
    "ls": [
        "dbt",
        "ls",
        "--resource-type",
        "model",
        "--select",
        "state:modified+",
        "--output",
        "json",
    ],
}


if __name__ == "__main__":
    # Instance contains all required information to execute commands, make requests, etc
    config = Config()

    # Profile allows us to run dbt commands
    create_dbt_cloud_profile(config)

    logger.info("Compiling code for any modified nodes...")
    _ = subprocess.run(DBT_COMMANDS["compile"], capture_output=True)

    # Retrieve compiled code for anything modified in the PR
    target_nodes = get_target_compiled_code()

    # Exit early, don't trigger job if nothing is actually modified here.
    if not target_nodes:
        logger.info("Nothing modified so exiting early without triggering a CI job.")
        sys.exit(0)

    # Retrieve compiled code for modified models in the deferred environment
    source_nodes = get_source_compiled_code(config, list(target_nodes.keys()))

    # Combine dictionaries
    all_nodes = {
        node_name: {**target_nodes[node_name], **source_nodes[node_name]}
        for node_name in target_nodes.keys() & source_nodes.keys()
    }

    if not all_nodes:
        logger.info(
            f"Modified resources `{', '.join(target_nodes.keys())}` were not found in "
            "the deferred environment via the Discovery API. This most likely means "
            "that the resource(s) have not yet been run in the deferred environment."
        )
        run = trigger_job(config)

    else:
        # Get all affected nodes, including downstream, by running dbt ls
        logger.info("Running dbt command `dbt ls` to find all affected nodes...")
        result = subprocess.run(DBT_COMMANDS["ls"], capture_output=True, text=True)
        all_unique_ids = get_all_unique_ids(result, list(target_nodes.keys()))

        # Create NodeManager instance and find models to exclude
        node_manager = NodeManager(config, all_nodes, all_unique_ids)
        excluded_nodes = node_manager.get_excluded_nodes()

        # Trigger job with the excluded_nodes
        run = trigger_job(config, excluded_nodes=excluded_nodes)

    try:
        run_status = run["status"]
    except Exception:
        sys.exit(1)

    if run_status in (JobRunStatus.ERROR, JobRunStatus.CANCELLED):
        sys.exit(1)

    sys.exit(0)
