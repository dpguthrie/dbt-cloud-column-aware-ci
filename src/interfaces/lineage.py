from typing import TYPE_CHECKING, List, Protocol, Set

if TYPE_CHECKING:
    from src.models.node import Node


class LineageServiceProtocol(Protocol):
    def get_node_lineage(self, nodes: List["Node"]) -> Set[str]: ...

    def get_column_lineage(self, node_id: str, column_name: str) -> Set[str]: ...
