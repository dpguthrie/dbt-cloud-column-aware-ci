from unittest.mock import MagicMock

import pytest

from src.models.column_tracker import ColumnTracker
from src.models.node import Node


@pytest.fixture
def mock_node():
    """Create a mock Node instance with column changes."""
    node = MagicMock(spec=Node)
    node.unique_id = "model.my_project.test_model"
    node.column_changes = {"column1", "column2"}
    return node


def test_column_tracker_initialization(mock_lineage_service):
    """Test ColumnTracker initialization."""
    tracker = ColumnTracker(mock_lineage_service)
    assert tracker._tracked_columns == set()
    assert tracker._impacted_ids == set()
    assert tracker._lineage_service == mock_lineage_service


def test_track_node_columns_new_columns(mock_lineage_service, mock_node):
    """Test tracking new columns in a node."""
    # Setup mock lineage service to return some impacted nodes
    mock_lineage_service.get_column_lineage.return_value = {
        "model.my_project.downstream_model1",
        "model.my_project.downstream_model2",
    }

    tracker = ColumnTracker(mock_lineage_service)
    impacted_ids = tracker.track_node_columns(mock_node)

    # Verify the results
    expected_impacted_ids = {
        "model.my_project.downstream_model1",
        "model.my_project.downstream_model2",
    }
    expected_tracked_columns = {"model.my_project.test_model.column1", "model.my_project.test_model.column2"}

    assert tracker._tracked_columns == expected_tracked_columns
    assert tracker._impacted_ids == expected_impacted_ids
    assert impacted_ids == expected_impacted_ids

    # Verify lineage service was called correctly
    assert mock_lineage_service.get_column_lineage.call_count == 2
    mock_lineage_service.get_column_lineage.assert_any_call(
        "model.my_project.test_model", "COLUMN1"
    )
    mock_lineage_service.get_column_lineage.assert_any_call(
        "model.my_project.test_model", "COLUMN2"
    )


def test_track_node_columns_already_tracked(mock_lineage_service, mock_node):
    """Test tracking columns that have already been tracked."""
    tracker = ColumnTracker(mock_lineage_service)

    # Pre-populate tracked columns
    tracker._tracked_columns.add("model.my_project.test_model.column1")

    # First call to get_column_lineage returns some impacted nodes
    mock_lineage_service.get_column_lineage.return_value = {
        "model.my_project.downstream_model1"
    }

    impacted_ids = tracker.track_node_columns(mock_node)

    # Verify the results
    expected_tracked_columns = {
        "model.my_project.test_model.column1",
        "model.my_project.test_model.column2",
    }
    expected_impacted_ids = {"model.my_project.downstream_model1"}

    assert tracker._tracked_columns == expected_tracked_columns
    assert tracker._impacted_ids == expected_impacted_ids
    assert impacted_ids == expected_impacted_ids

    # Verify lineage service was called only once (for column2)
    mock_lineage_service.get_column_lineage.assert_called_once_with(
        "model.my_project.test_model", "COLUMN2"
    )


def test_impacted_ids_property(mock_lineage_service):
    """Test the impacted_ids property."""
    tracker = ColumnTracker(mock_lineage_service)

    # Set some impacted IDs
    expected_ids = {"model1", "model2"}
    tracker._impacted_ids = expected_ids.copy()

    assert tracker.impacted_ids == expected_ids
    # Ensure we get a copy of the set, not the original
    assert tracker.impacted_ids is not tracker._impacted_ids


def test_column_name_for_dialect(mock_lineage_service):
    """Test column name handling for different dialects."""
    tracker = ColumnTracker(mock_lineage_service)

    # Test Snowflake dialect (should uppercase)
    mock_lineage_service.config.dialect = "snowflake"
    assert tracker._column_name_for_dialect("test_column") == "TEST_COLUMN"
    assert tracker._column_name_for_dialect("MixedCase") == "MIXEDCASE"

    # Test other dialect (should return unchanged)
    mock_lineage_service.config.dialect = "bigquery"
    assert tracker._column_name_for_dialect("test_column") == "test_column"
    assert tracker._column_name_for_dialect("MixedCase") == "MixedCase"
