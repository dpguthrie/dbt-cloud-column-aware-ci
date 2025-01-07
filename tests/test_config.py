from unittest.mock import patch

import pytest

from src.config import Config


def test_config_initialization(mock_config):
    """Test Config initialization with all required fields."""
    assert mock_config.dbt_cloud_account_id == "43786"
    assert mock_config.dbt_cloud_job_id == "567183"
    assert mock_config.dbt_cloud_host == "cloud.getdbt.com"
    assert mock_config.dbt_cloud_project_id == "270542"
    assert mock_config.dbt_cloud_project_name == "Main"
    assert mock_config.dbt_cloud_token_name == "cloud-cli-6d65"
    assert mock_config.dbt_cloud_environment_id == "218762"


def test_config_from_env():
    """Test Config creation from environment variables."""
    env_vars = {
        "INPUT_DBT_CLOUD_HOST": "cloud.getdbt.com",
        "INPUT_DBT_CLOUD_SERVICE_TOKEN": "test_token",
        "INPUT_DBT_CLOUD_PROJECT_ID": "270542",
        "INPUT_DBT_CLOUD_PROJECT_NAME": "Main",
        "INPUT_DBT_CLOUD_TOKEN_NAME": "cloud-cli-6d65",
        "INPUT_DBT_CLOUD_TOKEN_VALUE": "test_token_value",
        "INPUT_DBT_CLOUD_ACCOUNT_ID": "43786",
        "INPUT_DBT_CLOUD_JOB_ID": "567183",
        "INPUT_DIALECT": "snowflake",
    }

    with (
        patch.dict("os.environ", env_vars, clear=True),
        patch("src.config.Config._set_deferring_environment_id") as mock_set_env,
    ):
        config = Config.from_env()

        assert config.dbt_cloud_host == "cloud.getdbt.com"
        assert config.dbt_cloud_service_token == "test_token"
        assert config.dbt_cloud_project_id == "270542"
        assert config.dbt_cloud_account_id == "43786"
        mock_set_env.assert_called_once()


def test_config_from_env_with_env_id():
    """Test Config creation from environment variables."""
    env_vars = {
        "INPUT_DBT_CLOUD_HOST": "cloud.getdbt.com",
        "INPUT_DBT_CLOUD_SERVICE_TOKEN": "test_token",
        "INPUT_DBT_CLOUD_PROJECT_ID": "270542",
        "INPUT_DBT_CLOUD_PROJECT_NAME": "Main",
        "INPUT_DBT_CLOUD_TOKEN_NAME": "cloud-cli-6d65",
        "INPUT_DBT_CLOUD_TOKEN_VALUE": "test_token_value",
        "INPUT_DBT_CLOUD_ACCOUNT_ID": "43786",
        "INPUT_DBT_CLOUD_JOB_ID": "567183",
        "INPUT_DBT_CLOUD_ENVIRONMENT_ID": "218762",
        "INPUT_DIALECT": "snowflake",
    }

    with (
        patch.dict("os.environ", env_vars, clear=True),
        patch("src.config.Config._set_deferring_environment_id") as mock_set_env,
    ):
        config = Config.from_env()

        assert config.dbt_cloud_host == "cloud.getdbt.com"
        assert config.dbt_cloud_service_token == "test_token"
        assert config.dbt_cloud_project_id == "270542"
        assert config.dbt_cloud_account_id == "43786"
        mock_set_env.assert_not_called()


def test_config_missing_env_vars():
    """Test Config creation with missing environment variables."""
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        assert "Missing required environment variables:" in str(exc_info.value)


def test_set_deferring_environment_id(mock_config):
    """Test setting deferring environment ID from dbt Cloud API."""
    mock_response = {"data": {"deferring_environment_id": "218762"}}

    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", return_value=mock_response
    ):
        mock_config._set_deferring_environment_id()
        assert mock_config.dbt_cloud_environment_id == "218762"


def test_set_deferring_environment_id_api_error(mock_config):
    """Test handling of API error when setting deferring environment ID."""
    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", side_effect=Exception("API Error")
    ):
        with pytest.raises(Exception) as exc_info:
            mock_config._set_deferring_environment_id()

        assert "An error occurred making a request to dbt Cloud" in str(exc_info.value)


def test_set_deferring_environment_id_missing_data(mock_config):
    """Test handling of missing data in API response."""
    mock_response = {"data": {}}  # Missing deferring_environment_id

    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", return_value=mock_response
    ):
        with pytest.raises(Exception) as exc_info:
            mock_config._set_deferring_environment_id()

        assert (
            "An error occurred retrieving your job's deferring environment ID"
            in str(exc_info.value)
        )
