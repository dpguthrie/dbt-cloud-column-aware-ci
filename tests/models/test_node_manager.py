# stdlib
from typing import Dict, Generator
from unittest.mock import MagicMock, patch

# third party
import pytest

# first party
from src.models.node import Node, NodeManager
from src.services.lineage_service import LineageService


@pytest.fixture
def mock_lineage_service(mock_config) -> LineageService:
    """Create a mock lineage service."""
    service = MagicMock(spec=LineageService)
    service.config = mock_config
    return service


@pytest.fixture
def sample_nodes() -> Dict[str, Dict[str, str]]:
    """Create sample node data for testing."""
    return {
        "model.project.unchanged_model": {
            "unique_id": "model.project.unchanged_model",
            "target_code": "SELECT id FROM table1",
            "source_code": "SELECT id FROM table1",
        },
        "model.project.column_change_model": {
            "unique_id": "model.project.column_change_model",
            "target_code": "SELECT id, name FROM table2",
            "source_code": "SELECT id FROM table2",
        },
        "model.project.structural_change_model": {
            "unique_id": "model.project.structural_change_model",
            "target_code": "SELECT id FROM new_table",
            "source_code": "SELECT id FROM old_table",
        },
    }


@pytest.fixture
def node_manager(
    mock_config, mock_lineage_service, mock_column_tracker, sample_nodes
) -> NodeManager:
    """Create a NodeManager instance with sample data."""
    all_unique_ids = {
        "model.project.unchanged_model",
        "model.project.column_change_model",
        "model.project.structural_change_model",
        "model.project.downstream1",
        "model.project.downstream2",
    }
    return NodeManager(
        config=mock_config,
        all_nodes=sample_nodes,
        all_unique_ids=all_unique_ids,
        lineage_service=mock_lineage_service,
    )


@pytest.fixture
def mock_column_tracker(mock_lineage_service) -> Generator[MagicMock, None, None]:
    """Create a mock column tracker that patches the ColumnTracker class."""
    with patch("src.models.column_tracker.ColumnTracker") as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance


def test_node_manager_initialization(
    node_manager: NodeManager, sample_nodes: Dict[str, Dict[str, str]]
) -> None:
    """Test that NodeManager is properly initialized."""
    assert len(node_manager.nodes) == len(sample_nodes)
    assert isinstance(node_manager.nodes[0], Node)
    assert node_manager.all_unique_ids == {
        "model.project.unchanged_model",
        "model.project.column_change_model",
        "model.project.structural_change_model",
        "model.project.downstream1",
        "model.project.downstream2",
    }


def test_get_excluded_nodes_no_changes(
    node_manager: NodeManager, mock_lineage_service: LineageService
) -> None:
    """Test that all nodes are excluded when there are no changes."""
    # Create new nodes list
    node_manager._node_dict = {
        "model.project.test1": Node(
            unique_id="model.project.test1",
            target_code="SELECT id FROM table1",
            source_code="SELECT id FROM table1",
            dialect="snowflake",
        ),
        "model.project.test2": Node(
            unique_id="model.project.test2",
            target_code="SELECT id FROM table2",
            source_code="SELECT id FROM table2",
            dialect="snowflake",
        ),
    }

    mock_lineage_service.get_column_lineage.return_value = set()
    mock_lineage_service.get_node_lineage.return_value = set()

    excluded = node_manager.get_excluded_nodes()

    # All downstream nodes should be excluded
    assert "downstream1" in excluded
    assert "downstream2" in excluded
    assert not mock_lineage_service.get_column_lineage.called
    assert not mock_lineage_service.get_node_lineage.called


def test_get_excluded_nodes_with_column_changes(
    node_manager: NodeManager,
    mock_lineage_service: LineageService,
    mock_column_tracker: MagicMock,
) -> None:
    """Test handling of models with column changes."""
    # Setup mock column tracker to indicate downstream1 is affected by column changes
    mock_column_tracker.track_node_columns.return_value = {"model.project.downstream1"}

    # Setup mock lineage service to return affected columns
    mock_lineage_service.get_column_lineage.return_value = {"name"}

    excluded = node_manager.get_excluded_nodes()

    # downstream1 is affected by column change, downstream2 can be excluded
    assert "downstream2" in excluded


def test_get_excluded_nodes_with_structural_changes(
    node_manager: NodeManager, mock_lineage_service: LineageService
) -> None:
    """Test handling of models with structural changes."""
    # Setup mock lineage service responses
    mock_lineage_service.get_node_lineage.return_value = {
        "model.project.downstream1",
        "model.project.downstream2",
    }

    excluded = node_manager.get_excluded_nodes()

    assert len(excluded) == 3

    # Verify node lineage was checked
    mock_lineage_service.get_node_lineage.assert_called_once()


def test_get_excluded_nodes_mixed_changes(
    node_manager: NodeManager,
    mock_lineage_service: LineageService,
    mock_column_tracker: MagicMock,
) -> None:
    """Test handling of models with both column and structural changes."""
    # Setup mock lineage service responses
    mock_column_tracker.track_node_columns.return_value = {"model.project.downstream1"}
    mock_lineage_service.get_node_lineage.return_value = {
        "model.project.downstream1",
        "model.project.downstream2",
    }

    excluded = node_manager.get_excluded_nodes()

    # No nodes should be excluded due to structural change
    assert len(excluded) == 3


def test_node_manager_empty_nodes(mock_config) -> None:
    """Test NodeManager behavior with empty node list."""
    node_manager = NodeManager(config=mock_config, all_nodes={}, all_unique_ids=set())

    excluded = node_manager.get_excluded_nodes()
    assert excluded == []


def test_node_manager_invalid_nodes(mock_config) -> None:
    """Test NodeManager behavior with invalid node data."""
    invalid_nodes = {
        "model.project.invalid": {
            "unique_id": "model.project.invalid",
            # Missing required fields
        }
    }

    with pytest.raises(KeyError):
        NodeManager(
            config=mock_config,
            all_nodes=invalid_nodes,
            all_unique_ids={"model.project.invalid"},
        )
