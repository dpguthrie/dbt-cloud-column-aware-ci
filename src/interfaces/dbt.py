# stdlib
from typing import Dict, List, Protocol, Set, runtime_checkable


@runtime_checkable
class DbtRunnerProtocol(Protocol):
    """Protocol defining the interface for dbt command runners."""

    def compile_models(self) -> None:
        """Compile modified models using dbt."""
        ...

    def get_target_compiled_code(self) -> Dict[str, Dict[str, str]]:
        """Get compiled code from run_results.json."""
        ...

    def get_source_compiled_code(
        self, unique_ids: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """Get compiled code from the deferring environment."""
        ...

    def get_all_unique_ids(self, modified_unique_ids: List[str]) -> Set[str]:
        """Get all unique IDs affected by changes."""
        ...
