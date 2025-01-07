# stdlib
import os
from typing import Dict
from unittest.mock import MagicMock

# third party
import pytest

# first party
from src.config import Config
from src.interfaces.dbt import DbtRunnerProtocol
from src.interfaces.discovery import DiscoveryClientProtocol
from src.interfaces.lineage import LineageServiceProtocol


@pytest.fixture
def mock_config() -> Config:
    """Create a mock configuration object."""
    return Config(
        dbt_cloud_account_id="43786",
        dbt_cloud_job_id="567183",
        dbt_cloud_host="cloud.getdbt.com",
        dbt_cloud_service_token=os.environ["DBT_CLOUD_SERVICE_TOKEN"],
        dbt_cloud_project_id="270542",
        dbt_cloud_project_name="Main",
        dbt_cloud_token_name="cloud-cli-6d65",
        dbt_cloud_token_value=os.environ["DBT_CLOUD_TOKEN_VALUE"],
        dbt_cloud_environment_id="218762",
        dialect="snowflake",
    )


@pytest.fixture
def mock_discovery_client() -> DiscoveryClientProtocol:
    """Create a mock Discovery API client."""
    client = MagicMock(spec=DiscoveryClientProtocol)

    # Setup default return values
    client.get_column_lineage.return_value = []
    client.get_node_lineage.return_value = set()
    client.get_compiled_code.return_value = {}

    return client


@pytest.fixture
def mock_dbt_runner() -> DbtRunnerProtocol:
    """Create a mock dbt runner."""
    runner = MagicMock(spec=DbtRunnerProtocol)

    # Setup default return values
    runner.get_target_compiled_code.return_value = {}
    runner.get_source_compiled_code.return_value = {}
    runner.get_all_unique_ids.return_value = set()

    return runner


@pytest.fixture
def mock_lineage_service() -> LineageServiceProtocol:
    """Create a mock lineage service."""
    service = MagicMock(spec=LineageServiceProtocol)

    # Setup default return values
    service.get_column_lineage.return_value = set()
    service.get_node_lineage.return_value = set()
    service.get_compiled_code.return_value = {}

    return service


@pytest.fixture
def sample_compiled_nodes() -> Dict[str, Dict[str, str]]:
    """Create sample compiled node data for testing."""
    return {
        "model.my_project.first_model": {
            "unique_id": "model.my_project.first_model",
            "target_code": "SELECT * FROM table1",
            "source_code": "SELECT * FROM table1",
        },
        "model.my_project.second_model": {
            "unique_id": "model.my_project.second_model",
            "target_code": "SELECT id, name FROM table2",
            "source_code": "SELECT id FROM table2",
        },
    }
