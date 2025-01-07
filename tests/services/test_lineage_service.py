# stdlib

# third party
import pytest

# first party
from src.models.node import Node
from src.services.lineage_service import LineageService


@pytest.fixture
def lineage_service(mock_config, mock_discovery_client) -> LineageService:
    """Create a LineageService instance with mocked dependencies."""
    return LineageService(config=mock_config, _discovery_client=mock_discovery_client)


@pytest.fixture
def sample_nodes() -> list[Node]:
    """Create sample nodes for testing."""
    return [
        Node(
            unique_id="model.project.test_model",
            target_code="SELECT id, name FROM table",
            source_code="SELECT id FROM table",
            dialect="snowflake",
        ),
        Node(
            unique_id="model.project.other_model",
            target_code="SELECT * FROM other_table",
            source_code="SELECT * FROM other_table",
            dialect="snowflake",
        ),
    ]


def test_get_column_lineage(lineage_service: LineageService) -> None:
    """Test column lineage retrieval."""
    mock_lineage = [
        {"nodeUniqueId": "model.project.downstream1", "relationship": "child"},
        {"nodeUniqueId": "model.project.downstream2", "relationship": "child"},
        {"nodeUniqueId": "model.project.parent", "relationship": "parent"},
    ]

    lineage_service._discovery_client.get_column_lineage.return_value = mock_lineage

    result = lineage_service.get_column_lineage(
        unique_id="model.project.test_model", column_name="test_column"
    )

    assert len(result) == 2
    assert "model.project.downstream1" in result
    assert "model.project.downstream2" in result
    assert "model.project.parent" not in result


def test_get_node_lineage(
    lineage_service: LineageService, sample_nodes: list[Node]
) -> None:
    """Test node lineage retrieval."""
    expected_lineage = {"model.project.downstream1", "model.project.downstream2"}
    lineage_service._discovery_client.get_node_lineage.return_value = expected_lineage

    result = lineage_service.get_node_lineage(sample_nodes)

    assert result == expected_lineage
    lineage_service._discovery_client.get_node_lineage.assert_called_once_with(
        lineage_service.config.dbt_cloud_environment_id, ["test_model", "other_model"]
    )


def test_get_compiled_code(lineage_service: LineageService) -> None:
    """Test compiled code retrieval."""
    mock_compiled_code = {
        "model.project.test_model": {"source_code": "SELECT * FROM table"}
    }
    lineage_service._discovery_client.get_compiled_code.return_value = (
        mock_compiled_code
    )

    result = lineage_service.get_compiled_code(["model.project.test_model"])

    assert result == mock_compiled_code
    lineage_service._discovery_client.get_compiled_code.assert_called_once_with(
        lineage_service.config.dbt_cloud_environment_id, ["model.project.test_model"]
    )
