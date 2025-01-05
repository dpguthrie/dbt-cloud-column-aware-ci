# stdlib
import logging
from dataclasses import dataclass
from typing import Dict, List, Set

# first party
from src.config import Config
from src.discovery_api_queries import QUERIES
from src.interfaces.discovery import DiscoveryClientProtocol

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryClient(DiscoveryClientProtocol):
    """
    Client for interacting with the dbt Cloud Discovery API.

    This class implements the DiscoveryClientProtocol interface, providing
    methods to fetch lineage information and compiled code from the Discovery API.

    Attributes:
        config: Configuration object containing dbt Cloud credentials and settings
    """

    config: Config

    def get_column_lineage(
        self, environment_id: str, node_id: str, column_name: str
    ) -> List[Dict[str, str]]:
        """
        Get lineage information for a specific column.

        Makes a request to the Discovery API to fetch information about which
        downstream nodes depend on a specific column.

        Args:
            environment_id: The dbt Cloud environment ID
            node_id: The unique identifier of the node containing the column
            column_name: The name of the column to check

        Returns:
            List[Dict[str, str]]: List of dictionaries containing lineage information
                                Each dictionary contains 'nodeUniqueId' and 'relationship'
        """
        variables = {
            "environmentId": environment_id,
            "nodeUniqueId": node_id,
            "filters": {"columnName": column_name.upper()},
        }

        results = self.config.dbtc_client.metadata.query(
            QUERIES["column_lineage"], variables
        )

        try:
            return results["data"]["column"]["lineage"]
        except Exception as e:
            logger.error(
                f"Error occurred retrieving column lineage for {column_name} "
                f"in {node_id}:\n{e}"
            )
            return []

    def get_node_lineage(self, environment_id: str, node_names: List[str]) -> Set[str]:
        """
        Get lineage information for multiple nodes.

        Makes a request to the Discovery API to fetch information about which
        downstream nodes depend on the specified nodes.

        Args:
            environment_id: The dbt Cloud environment ID
            node_names: List of node names to check for lineage

        Returns:
            Set[str]: Set of unique IDs for all downstream dependent nodes
        """
        names_str = "+ ".join(node_names) + "+"
        variables = {
            "environmentId": environment_id,
            "filter": {
                "select": f"--select {names_str}",
                "exclude": f"--exclude {' '.join(node_names)}",
                "types": ["Model"],
            },
        }

        results = self.config.dbtc_client.metadata.query(
            QUERIES["node_lineage"], variables
        )
        logger.info(f"Results:\n{results}")

        try:
            return {
                r["uniqueId"]
                for r in results["data"]["environment"]["applied"]["lineage"]
            }
        except Exception:
            logger.error(f"Error occurred retrieving lineage for {names_str}")
            logger.error(f"Response:\n{results}")
            return set()

    def get_compiled_code(
        self, environment_id: str, unique_ids: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Get compiled code for specified nodes.

        Makes a request to the Discovery API to fetch the compiled SQL code
        for the specified nodes.

        Args:
            environment_id: The dbt Cloud environment ID
            unique_ids: List of node unique IDs to fetch compiled code for

        Returns:
            Dict[str, Dict[str, str]]: Dictionary mapping node IDs to their properties,
                                     including compiled code
        """
        variables = {
            "first": 500,
            "after": None,
            "environmentId": environment_id,
            "filter": {"uniqueIds": unique_ids},
        }

        nodes = self.config.dbtc_client.metadata.query(
            QUERIES["compiled_code"], variables, paginated_request_to_list=True
        )

        if nodes and "node" not in nodes[0]:
            logger.error(
                "Error encountered making request to discovery API.\n"
                f"Error message:\n{nodes[0]['message']}\n"
                f"Full response:\n{nodes[0]}"
            )
            return {}

        compiled_nodes: Dict[str, Dict[str, str]] = {}
        for node in nodes:
            unique_id = node["node"]["uniqueId"]
            compiled_code = node["node"]["compiledCode"]
            compiled_nodes[unique_id] = {"source_code": compiled_code}
            logger.info(f"Compiled code found for `{unique_id}`")

        return compiled_nodes
