# stdlib
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:  # pragma: no cover
    from src.models.node import Node
    from src.services.lineage_service import LineageService

logger = logging.getLogger(__name__)


@dataclass
class ColumnTracker:
    """
    Tracks column-level changes across dbt models and their impacts.

    This class maintains a record of which columns have been analyzed and
    which nodes are impacted by column-level changes.

    Attributes:
        lineage_service: Service for querying model lineage information
        _tracked_columns: Set of column identifiers that have been analyzed
        _impacted_ids: Set of node IDs impacted by column changes
    """

    _lineage_service: "LineageService"
    _tracked_columns: Set[str] = field(default_factory=set)
    _impacted_ids: Set[str] = field(default_factory=set)

    def track_node_columns(self, node: "Node") -> Set[str]:
        """
        Track columns for a node and identify impacted downstream nodes.

        For each changed column in the node, this method:
        1. Checks if the column has already been analyzed
        2. If not, finds downstream nodes that depend on this column
        3. Records the column as tracked and updates impacted nodes

        Args:
            node: The Node instance containing column changes to analyze

        Returns:
            Set[str]: Set of node IDs impacted by the column changes
        """
        impacted_ids: Set[str] = set()

        for column_name in node.column_changes:
            node_column = f"{node.unique_id}.{column_name}"
            if node_column not in self._tracked_columns:
                logger.info(
                    f"Column `{column_name}` in node `{node.unique_id}` "
                    f"has a change. Finding downstream nodes using this column ..."
                )
                impacted_ids.update(
                    self._lineage_service.get_column_lineage(
                        node.unique_id, column_name
                    )
                )
                self._tracked_columns.add(node_column)
                self._impacted_ids.update(impacted_ids)

        return impacted_ids

    @property
    def impacted_ids(self):
        """
        Get all node IDs impacted by tracked column changes.

        Returns:
            Set[str]: Set of all node IDs that are impacted by any tracked
                     column changes
        """
        return self._impacted_ids.copy()
