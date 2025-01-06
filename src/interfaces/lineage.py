from typing import Protocol, Set

from typing_extensions import TypeAlias

# Use forward reference with string literal for Node type
Node: TypeAlias = "Node"  # type: ignore


class LineageServiceProtocol(Protocol):
    def get_node_lineage(self, nodes: list["Node"]) -> Set[str]: ...

    def get_column_lineage(self, node_id: str, column_name: str) -> Set[str]: ...
