from typing import TYPE_CHECKING, Dict, List, Protocol, Set

if TYPE_CHECKING:  # pragma: no cover
    from src.models.node import Node
    from src.config import Config

class LineageServiceProtocol(Protocol):
    config: "Config"

    def get_node_lineage(self, nodes: List["Node"]) -> Set[str]: ...

    def get_column_lineage(self, node_id: str, column_name: str) -> Set[str]: ...

    def get_compiled_code(self, unique_ids: List[str]) -> Dict[str, Dict[str, str]]: ...
