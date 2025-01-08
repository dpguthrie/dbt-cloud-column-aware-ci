# stdlib
from unittest.mock import MagicMock

# third party
import pytest

# first party
from src.services.discovery_client import DiscoveryClient


@pytest.fixture
def discovery_client(mock_config) -> DiscoveryClient:
    """Create a DiscoveryClient instance with a mock config."""
    return DiscoveryClient(config=mock_config)


def test_get_column_lineage(discovery_client: DiscoveryClient) -> None:
    """Test column lineage retrieval."""
    # Mock response data
    mock_response = {
        "data": {
            "column": {
                "lineage": [
                    {
                        "nodeUniqueId": "model.project.downstream1",
                        "relationship": "child",
                    },
                    {
                        "nodeUniqueId": "model.project.downstream2",
                        "relationship": "child",
                    },
                ]
            }
        }
    }

    # Mock the metadata query
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        return_value=mock_response
    )

    # Test the method
    result = discovery_client.get_column_lineage(
        environment_id="123",
        node_id="model.project.test",
        column_name="test_column",
    )

    assert len(result) == 2
    assert result[0]["nodeUniqueId"] == "model.project.downstream1"
    assert result[1]["nodeUniqueId"] == "model.project.downstream2"


def test_get_node_lineage(discovery_client: DiscoveryClient) -> None:
    """Test node lineage retrieval."""
    # Mock response data
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {"uniqueId": "model.project.downstream1"},
                        {"uniqueId": "model.project.downstream2"},
                    ]
                }
            }
        }
    }

    # Mock the metadata query
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        return_value=mock_response
    )

    # Test the method
    result = discovery_client.get_node_lineage(
        environment_id="123",
        node_names=["test_model"],
    )

    assert len(result) == 2
    assert "model.project.downstream1" in result
    assert "model.project.downstream2" in result


def test_get_compiled_code(discovery_client: DiscoveryClient) -> None:
    """Test compiled code retrieval."""
    # Mock response data
    mock_response = [
        {
            "node": {
                "uniqueId": "model.project.test1",
                "compiledCode": "SELECT * FROM table1",
            }
        },
        {
            "node": {
                "uniqueId": "model.project.test2",
                "compiledCode": "SELECT * FROM table2",
            }
        },
    ]

    # Mock the metadata query
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        return_value=mock_response
    )

    # Test the method
    result = discovery_client.get_compiled_code(
        environment_id="123", unique_ids=["model.project.test1", "model.project.test2"]
    )

    assert len(result) == 2
    assert result["model.project.test1"]["source_code"] == "SELECT * FROM table1"
    assert result["model.project.test2"]["source_code"] == "SELECT * FROM table2"


def test_get_compiled_code_error_response(discovery_client: DiscoveryClient) -> None:
    """Test compiled code retrieval with error response."""
    # Mock error response
    mock_response = [{"message": "API Error"}]

    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        return_value=mock_response
    )

    result = discovery_client.get_compiled_code(
        environment_id="123", unique_ids=["model.project.test1"]
    )

    assert result == {}


def test_get_column_lineage_error(discovery_client: DiscoveryClient) -> None:
    """Test column lineage retrieval with error."""
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        side_effect=Exception("API Error")
    )

    result = discovery_client.get_column_lineage(
        environment_id="123", node_id="model.project.test", column_name="test_column"
    )

    assert result == []


def test_get_node_lineage_error(discovery_client: DiscoveryClient) -> None:
    """Test node lineage retrieval with error."""
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        side_effect=Exception("API Error")
    )

    result = discovery_client.get_node_lineage(
        environment_id="123", node_names=["test_model"]
    )

    assert result == set()


def test_get_compiled_code_error(discovery_client: DiscoveryClient) -> None:
    """Test compiled code retrieval with error."""
    discovery_client.config.dbtc_client.metadata.query = MagicMock(
        side_effect=Exception("API Error")
    )

    result = discovery_client.get_compiled_code(
        environment_id="123", unique_ids=["model.project.test1"]
    )

    assert result == {}
