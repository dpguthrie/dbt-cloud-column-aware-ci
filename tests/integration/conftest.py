# stdlib
import os
from pathlib import Path
from typing import Dict, Generator

# third party
import pytest
from _pytest.fixtures import FixtureRequest

# first party
from src.config import Config
from src.services.dbt_runner import DbtRunner
from src.services.discovery_client import DiscoveryClient
from src.services.lineage_service import LineageService
from src.services.orchestrator import CiOrchestrator


@pytest.fixture
def test_project_dir(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary dbt project directory."""
    project_dir = tmp_path / "test_dbt_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    with open(project_dir / "dbt_project.yml", "w") as f:
        f.write("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
        """)

    # Create models directory
    models_dir = project_dir / "models"
    models_dir.mkdir()

    # Change to project directory for test
    original_dir = os.getcwd()
    os.chdir(project_dir)

    yield project_dir

    # Cleanup
    os.chdir(original_dir)


@pytest.fixture
def integration_config() -> Config:
    """Create a configuration for integration testing."""
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
    )


@pytest.fixture
def discovery_client(integration_config: Config) -> DiscoveryClient:
    """Create a DiscoveryClient instance for integration testing."""
    return DiscoveryClient(config=integration_config)


@pytest.fixture
def dbt_runner(integration_config: Config) -> DbtRunner:
    """Create a DbtRunner instance for integration testing."""
    return DbtRunner(config=integration_config)


@pytest.fixture
def lineage_service(integration_config: Config) -> LineageService:
    """Create a LineageService instance for integration testing."""
    return LineageService(config=integration_config)


@pytest.fixture
def orchestrator(integration_config: Config) -> CiOrchestrator:
    """Create a CiOrchestrator instance for integration testing."""
    return CiOrchestrator(config=integration_config)
