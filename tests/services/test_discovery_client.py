# stdlib
from typing import Dict, List
from unittest.mock import MagicMock, patch

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
