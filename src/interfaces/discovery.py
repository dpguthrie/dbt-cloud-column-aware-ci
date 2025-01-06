# stdlib
from typing import Dict, List, Protocol, Set, runtime_checkable


@runtime_checkable
class DiscoveryClientProtocol(Protocol):
    """Protocol defining the interface for Discovery API clients."""

    def get_column_lineage(
        self, environment_id: str, node_id: str, column_name: str
    ) -> List[Dict[str, str]]:
        """Get lineage information for a specific column."""
        ...

    def get_node_lineage(self, environment_id: str, node_names: List[str]) -> Set[str]:
        """Get lineage information for multiple nodes."""
        ...

    def get_compiled_code(
        self, environment_id: str, unique_ids: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """Get compiled code for specified nodes."""
        ...
