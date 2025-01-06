# stdlib
import logging
from typing import Dict, List

# first party
from src.models.node import Node

logger = logging.getLogger(__name__)


class NodeFactory:
    """
    Factory class for creating Node instances from raw node data.

    This class provides static methods to create and manage Node instances,
    handling the conversion from raw data to Node objects.
    """

    @staticmethod
    def create_nodes(all_nodes: Dict[str, Dict[str, str]]) -> Dict[str, Node]:
        """
        Create Node instances from raw node data.

        Args:
            all_nodes: Dictionary mapping node IDs to their properties,
                      including source and target code

        Returns:
            Dict[str, Node]: Dictionary mapping node IDs to their corresponding
                            Node instances
        """
        logger.debug(
            "Creating node instances",
            extra={
                "input_node_count": len(all_nodes),
                "input_node_names": list(all_nodes.keys()),
            },
        )

        nodes = {k: Node(**v) for k, v in all_nodes.items() if v.get("source_code")}

        logger.info(
            "Created node instances",
            extra={
                "created_node_count": len(nodes),
                "created_node_names": list(nodes.keys()),
                "skipped_node_names": list(set(all_nodes.keys()) - set(nodes.keys())),
            },
        )
        return nodes

    @staticmethod
    def get_node_names(nodes: List[Node]) -> List[str]:
        """
        Extract node names from their unique IDs.

        Args:
            nodes: List of Node instances

        Returns:
            List[str]: List of node names extracted from their unique IDs
        """
        logger.debug("Extracting node names", extra={"node_count": len(nodes)})

        names = [node.unique_id.split(".")[-1] for node in nodes]

        logger.debug("Extracted node names", extra={"name_count": len(names)})
        return names
