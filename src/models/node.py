# stdlib
from __future__ import annotations

import logging
import typing as t
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional, Set

# third party
from sqlglot import diff, exp, parse_one
from sqlglot.diff import Insert

# first party
from src.config import Config
from src.models.breaking_change import BreakingChange

if TYPE_CHECKING:
    from src.models.node_factory import NodeFactory
    from src.services.lineage_service import LineageService

logger = logging.getLogger(__name__)


@dataclass
class Node:
    """
    Represents a dbt model node with its source and target SQL code.

    A Node contains the SQL code for both the current version (target) and the
    previous version (source) of a dbt model, along with analysis of changes
    between these versions.

    Attributes:
        unique_id: The unique identifier for the dbt model
        target_code: The SQL code for the current version
        source_code: The SQL code for the previous version
    """

    unique_id: str
    target_code: str
    source_code: str
    dialect: str

    def __post_init__(self) -> None:
        """
        Initialize the node by analyzing differences between source and target code.

        Parses both versions of the SQL code and identifies breaking changes
        between them.
        """
        self._source_exp = parse_one(self.source_code, dialect=self.dialect)
        self._target_exp = parse_one(self.target_code, dialect=self.dialect)
        try:
            self.changes = diff(self._source_exp, self._target_exp, delta_only=True)
        except Exception as e:
            logger.error(
                f"There was a problem creating a diff for `{self.unique_id}`.\n"
                f"Error: {e}\n\nSource: {self.source_code}\nTarget: {self.target_code}"
            )
            self.changes = []

        # All breaking changes from diff
        self.breaking_changes = self._get_breaking_changes()

        # We should ignore column level changes if there are any node level changes
        self.ignore_column_changes = any(
            bc for bc in self.breaking_changes if bc.column_name is None
        )

        if self.ignore_column_changes:
            self.column_changes: t.Set[str] = set()
        else:
            self.column_changes = {
                bc.column_name for bc in self.breaking_changes if bc.column_name
            }

    def _get_breaking_changes(self) -> list[BreakingChange]:
        """
        Identify breaking changes between source and target code.

        A breaking change is any modification that could affect downstream
        dependencies, such as column removals or renames.

        Returns:
            list[BreakingChange]: List of breaking changes found
        """
        breaking_changes: list[BreakingChange] = []
        inserts = {e.expression for e in self.changes if isinstance(e, Insert)}

        for edit in self.changes:
            if not isinstance(edit, Insert):
                breaking_changes.append(BreakingChange(edit))
            elif isinstance(edit.expression, exp.UDTF) or (
                not isinstance(edit.expression.parent, exp.Select)
                and edit.expression.parent not in inserts
            ):
                breaking_changes.append(BreakingChange(edit))

        return breaking_changes


class NodeFactory:
    @staticmethod
    def create_nodes(
        nodes_data: Dict[str, Dict[str, str]], dialect: str
    ) -> Dict[str, "Node"]:
        """Create Node instances from raw node data."""
        return {
            node_id: Node(
                unique_id=data["unique_id"],
                source_code=data["source_code"],
                target_code=data["target_code"],
                dialect=dialect,
            )
            for node_id, data in nodes_data.items()
        }


class NodeManager:
    """
    Manages a collection of dbt model nodes and their dependencies.

    This class handles the analysis of changes across multiple nodes and
    determines which downstream models are affected by these changes.

    Attributes:
        config: The configuration object containing dbt Cloud settings
        all_unique_ids: Set of all model IDs in the project
    """

    def __init__(
        self,
        config: Config,
        all_nodes: Dict[str, Dict[str, str]],
        all_unique_ids: Set[str],
        lineage_service: Optional["LineageService"] = None,
    ) -> None:
        """
        Initialize the NodeManager.

        Args:
            config: Configuration object with dbt Cloud settings
            all_nodes: Dictionary mapping node IDs to their properties
            all_unique_ids: Set of all model IDs in the project
        """
        from src.models.column_tracker import ColumnTracker
        from src.services.lineage_service import LineageService

        self.config = config
        self._lineage_service = lineage_service or LineageService(config)
        self._column_tracker = ColumnTracker(self._lineage_service)
        self._node_dict = NodeFactory.create_nodes(all_nodes, self.config.dialect)
        self.all_unique_ids = all_unique_ids
        self._all_impacted_unique_ids: t.Set[str] = set()

    @property
    def node_unique_ids(self) -> list[str]:
        """Get list of unique IDs for all managed nodes."""
        return list(self._node_dict.keys())

    @property
    def nodes(self) -> list[Node]:
        """Get list of all managed nodes."""
        return list(self._node_dict.values())

    def get_excluded_nodes(self) -> list[str]:
        """
        Get list of nodes that can be excluded from rebuilding.

        Analyzes changes in all nodes to determine which downstream models
        are not affected by the changes and can therefore be excluded from
        rebuilding.

        Returns:
            list[str]: List of node names that can be excluded
        """
        if not self.nodes:
            return list()

        if not self.all_unique_ids:
            return list()

        # Column level changes
        for node in self.nodes:
            if node.column_changes:
                self._all_impacted_unique_ids.update(
                    self._column_tracker.track_node_columns(node)
                )

        # Node level changes
        nodes = [node for node in self.nodes if node.ignore_column_changes]
        if nodes:
            logger.info("Some nodes were found to have node level breaking changes...")
            logger.info(f"Nodes: {', '.join([n.unique_id for n in nodes])}")
            self._all_impacted_unique_ids.update(
                self._lineage_service.get_node_lineage(nodes)
            )

        excluded_nodes = self.all_unique_ids - self._all_impacted_unique_ids
        return [em.split(".")[-1] for em in excluded_nodes]
