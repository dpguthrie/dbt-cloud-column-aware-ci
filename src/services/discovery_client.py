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
        logger.debug(
            "Fetching column lineage",
            extra={
                "environment_id": environment_id,
                "node_id": node_id,
                "column_name": column_name,
            },
        )

        try:
            variables = {
                "environmentId": environment_id,
                "nodeUniqueId": node_id,
                "filters": {"columnName": column_name.upper()},
            }

            lineage = self.config.dbtc_client.metadata.query(
                QUERIES["column_lineage"], variables
            )["data"]["column"]["lineage"]

            logger.info(
                "Retrieved column lineage",
                extra={
                    "node_id": node_id,
                    "column_name": column_name,
                    "lineage_count": len(lineage),
                },
            )
            return lineage

        except Exception as e:
            logger.error(
                "Failed to get column lineage",
                extra={"node_id": node_id, "column_name": column_name, "error": str(e)},
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
        logger.debug(
            "Fetching node lineage",
            extra={"environment_id": environment_id, "node_count": len(node_names)},
        )

        try:
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
            lineage = {
                r["uniqueId"]
                for r in results["data"]["environment"]["applied"]["lineage"]
            }

            logger.info(
                "Retrieved node lineage",
                extra={
                    "source_node_count": len(node_names),
                    "downstream_node_count": len(lineage),
                },
            )
            return lineage

        except Exception as e:
            logger.error(
                "Failed to get node lineage",
                extra={"node_names": node_names, "error": str(e)},
            )
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
        logger.debug(
            "Fetching compiled code",
            extra={"environment_id": environment_id, "node_count": len(unique_ids)},
        )

        try:
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
                    "Failed to retrieve compiled code",
                    extra={"error_message": nodes[0].get("message", "Unknown error")},
                )
                return {}

            compiled_nodes = {}
            for node in nodes:
                unique_id = node["node"]["uniqueId"]
                compiled_nodes[unique_id] = {
                    "source_code": node["node"]["compiledCode"]
                }

            logger.info(
                "Retrieved compiled code",
                extra={
                    "requested_count": len(unique_ids),
                    "retrieved_count": len(compiled_nodes),
                },
            )
            return compiled_nodes

        except Exception as e:
            logger.error(
                "Failed to get compiled code",
                extra={"unique_ids": unique_ids, "error": str(e)},
            )
            return {}
