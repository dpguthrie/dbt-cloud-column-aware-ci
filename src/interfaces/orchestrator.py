# stdlib
from typing import Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class OrchestratorProtocol(Protocol):
    """Protocol defining the interface for CI orchestrators."""

    def setup(self) -> None:
        """Set up the environment for running dbt commands."""
        ...

    def compile_and_get_nodes(self) -> Dict[str, Dict[str, str]]:
        """Compile models and get their compiled code."""
        ...

    def get_excluded_nodes(self, all_nodes: Dict[str, Dict[str, str]]) -> list[str]:
        """Get list of nodes to exclude from the build."""
        ...

    def trigger_and_check_job(self, excluded_nodes: Optional[list[str]] = None) -> bool:
        """Trigger a dbt Cloud job and check its status."""
        ...

    def run(self) -> bool:
        """Run the entire CI process."""
        ...
