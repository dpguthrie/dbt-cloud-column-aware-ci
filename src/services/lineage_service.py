# stdlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional, Set

# first party
from src.config import Config
from src.interfaces.lineage import LineageServiceProtocol
from src.services.discovery_client import DiscoveryClient

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.models.node import Node


@dataclass
class LineageService(LineageServiceProtocol):
    """
    Service for managing model lineage information.

    This class implements the LineageServiceProtocol interface, providing
    higher-level lineage operations and caching results where appropriate.

    Attributes:
        config: Configuration object containing dbt Cloud settings
        _discovery_client: Client for making Discovery API requests
    """

    config: Config
    _discovery_client: Optional[DiscoveryClient] = None

    def __post_init__(self) -> None:
        """Initialize the Discovery API client if not provided."""
        if self._discovery_client is None:
            self._discovery_client = DiscoveryClient(self.config)

    def get_column_lineage(self, unique_id: str, column_name: str) -> Set[str]:
        """
        Get downstream nodes that depend on a specific column.

        Queries the Discovery API to find all downstream nodes that use
        the specified column from the given node.

        Args:
            unique_id: The unique identifier of the node containing the column
            column_name: The name of the column to check

        Returns:
            Set[str]: Set of unique IDs for nodes that depend on this column
        """
        lineage = self._discovery_client.get_column_lineage(
            self.config.dbt_cloud_environment_id, unique_id, column_name
        )

        downstream_nodes = {
            node["nodeUniqueId"] for node in lineage if node["relationship"] == "child"
        }

        if downstream_nodes:
            logger.info(
                f"Column `{column_name}` in node `{unique_id}` is being used by the "
                f"following downstream nodes: `{', '.join(downstream_nodes)}`"
            )
        else:
            logger.info(
                f"Column `{column_name}` in node `{unique_id}` is NOT being used "
                "anywhere downstream."
            )

        return downstream_nodes

    def get_node_lineage(self, nodes: list["Node"]) -> Set[str]:
        """
        Get downstream nodes that depend on the given nodes.

        Queries the Discovery API to find all downstream nodes that depend
        on any of the specified nodes.

        Args:
            nodes: List of Node instances to check for dependencies

        Returns:
            Set[str]: Set of unique IDs for all downstream dependent nodes
        """
        names = [node.unique_id.split(".")[-1] for node in nodes]
        return self._discovery_client.get_node_lineage(
            self.config.dbt_cloud_environment_id, names
        )

    def get_compiled_code(self, unique_ids: list[str]) -> Dict[str, Dict[str, str]]:
        """
        Get compiled code for specified nodes.

        Retrieves the compiled SQL code for the specified nodes from the
        Discovery API.

        Args:
            unique_ids: List of node unique IDs to fetch compiled code for

        Returns:
            Dict[str, Dict[str, str]]: Dictionary mapping node IDs to their
                                      properties, including compiled code
        """
        return self._discovery_client.get_compiled_code(
            self.config.dbt_cloud_environment_id, unique_ids
        )
