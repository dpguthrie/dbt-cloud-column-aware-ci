# stdlib
import logging
from dataclasses import dataclass
from typing import Dict, Optional

# first party
from src.config import Config
from src.interfaces.orchestrator import OrchestratorProtocol
from src.models.node import NodeManager
from src.services.dbt_runner import DbtRunner
from src.utils import (
    JobRunStatus,
    create_dbt_cloud_profile,
    post_dry_run_message,
    trigger_job,
)

logger = logging.getLogger(__name__)


@dataclass
class CiOrchestrator(OrchestratorProtocol):
    """
    Orchestrates the CI process for dbt model changes.

    This class implements the OrchestratorProtocol interface, coordinating
    the entire CI workflow, including:
    - Setting up the environment
    - Compiling and analyzing model changes
    - Determining which models need to be rebuilt
    - Triggering and monitoring dbt Cloud jobs

    Attributes:
        config: Configuration object containing dbt Cloud settings
        dbt_runner: Service for executing dbt commands
    """

    config: Config
    dbt_runner: Optional[DbtRunner] = None

    def __post_init__(self) -> None:
        """Initialize the dbt runner if not provided."""
        if self.dbt_runner is None:
            self.dbt_runner = DbtRunner(self.config)

    def setup(self) -> None:
        """
        Set up the environment for running dbt commands.

        Creates necessary profiles and configurations for dbt to run
        in the CI environment.

        Raises:
            RuntimeError: If environment setup fails
        """
        create_dbt_cloud_profile(self.config)

    def compile_and_get_nodes(self) -> Dict[str, Dict[str, str]]:
        """
        Compile models and get their compiled code.

        This method:
        1. Compiles modified models
        2. Retrieves compiled code from the target environment
        3. Retrieves compiled code from the source environment
        4. Combines the results

        Returns:
            Dict[str, Dict[str, str]]: Dictionary mapping node IDs to their
                                      properties, including compiled code

        Raises:
            RuntimeError: If compilation fails
        """
        self.dbt_runner.compile_models()
        target_nodes = self.dbt_runner.get_target_compiled_code()

        if not target_nodes:
            logger.info("Nothing found in the run_results.json file.")
            return {}

        source_nodes = self.dbt_runner.get_source_compiled_code(
            list(target_nodes.keys())
        )

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

        return all_nodes

    def get_excluded_nodes(self, all_nodes: Dict[str, Dict[str, str]]) -> list[str]:
        """
        Get list of nodes to exclude from the build.

        Analyzes the changes in the provided nodes and determines which
        downstream models can safely be excluded from rebuilding.

        Args:
            all_nodes: Dictionary mapping node IDs to their properties

        Returns:
            list[str]: List of node names that can be excluded from rebuilding
        """
        all_unique_ids = self.dbt_runner.get_all_unique_ids(list(all_nodes.keys()))
        node_manager = NodeManager(self.config, all_nodes, all_unique_ids)
        return node_manager.get_excluded_nodes()

    def trigger_and_check_job(self, excluded_nodes: Optional[list[str]] = None) -> bool:
        """
        Trigger a dbt Cloud job and check its status.

        If dry_run is True, will only log what would happen instead of actually
        triggering the job.

        Args:
            excluded_nodes: Optional list of node names to exclude from the build

        Returns:
            bool: True if the job completed successfully (or if dry_run), False otherwise
        """
        if self.config.dry_run:
            post_dry_run_message(excluded_nodes)
            return True

        run = trigger_job(self.config, excluded_nodes=excluded_nodes)

        try:
            run_status = run["status"]
        except KeyError:
            logger.error("Failed to get job status")
            return False

        return run_status not in (JobRunStatus.ERROR, JobRunStatus.CANCELLED)

    def run(self) -> bool:
        """
        Run the entire CI process.

        This method orchestrates the complete CI workflow:
        1. Sets up the environment
        2. Compiles and analyzes changes
        3. Determines which models to exclude
        4. Triggers and monitors the dbt Cloud job

        Returns:
            bool: True if the process completed successfully, False otherwise
        """
        try:
            # Setup environment
            self.setup()

            # Compile and get nodes
            all_nodes = self.compile_and_get_nodes()
            if not all_nodes:
                return self.trigger_and_check_job()

            # Get excluded nodes
            excluded_nodes = self.get_excluded_nodes(all_nodes)

            # Trigger job and return status
            return self.trigger_and_check_job(excluded_nodes)

        except Exception as e:
            logger.error(f"Error during CI process: {e}")
            return False
