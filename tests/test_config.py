from unittest.mock import Mock, patch

import pytest

from src.config import Config


def test_config_initialization(mock_config):
    """Test Config initialization with all required fields."""
    assert mock_config.dbt_cloud_account_id == "43786"
    assert mock_config.dbt_cloud_job_id == "567183"
    assert mock_config.dbt_cloud_host == "cloud.getdbt.com"
    assert mock_config.dbt_cloud_token_name == "cloud-cli-6d65"
    assert mock_config.dbt_cloud_environment_id == 218762


def test_config_from_env():
    """Test Config creation from environment variables."""
    env_vars = {
        "INPUT_DBT_CLOUD_HOST": "cloud.getdbt.com",
        "INPUT_DBT_CLOUD_SERVICE_TOKEN": "test_token",
        "INPUT_DBT_CLOUD_TOKEN_NAME": "cloud-cli-6d65",
        "INPUT_DBT_CLOUD_TOKEN_VALUE": "test_token_value",
        "INPUT_DBT_CLOUD_ACCOUNT_ID": "43786",
        "INPUT_DBT_CLOUD_JOB_ID": "567183",
        "INPUT_DIALECT": "snowflake",
    }

    with (
        patch.dict("os.environ", env_vars, clear=True),
        patch("src.config.Config._set_fields_from_dbtc_client") as mock_set_env,
    ):
        config = Config.from_env()

        assert config.dbt_cloud_host == "cloud.getdbt.com"
        assert config.dbt_cloud_service_token == "test_token"
        mock_set_env.assert_called_once()


def test_config_missing_env_vars():
    """Test Config creation with missing environment variables."""
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        assert "Missing required environment variables:" in str(exc_info.value)


def test_set_fields_from_dbtc_client_missing_data(mock_config):
    """Test handling of missing data in API response."""
    mock_response = {"data": {}}  # Missing deferring_environment_id

    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", return_value=mock_response
    ):
        with pytest.raises(Exception) as exc_info:
            mock_config._set_fields_from_dbtc_client()

        assert "An error occurred retrieving your job's data" in str(exc_info.value)


def test_config_dry_run(mock_config):
    # Default value from mock_config fixture should be False
    assert mock_config.dry_run is False

    # Test creating config with dry_run=True
    env_vars = {
        "INPUT_DBT_CLOUD_HOST": mock_config.dbt_cloud_host,
        "INPUT_DBT_CLOUD_ACCOUNT_ID": mock_config.dbt_cloud_account_id,
        "INPUT_DBT_CLOUD_SERVICE_TOKEN": mock_config.dbt_cloud_service_token,
        "INPUT_DBT_CLOUD_TOKEN_NAME": mock_config.dbt_cloud_token_name,
        "INPUT_DBT_CLOUD_TOKEN_VALUE": mock_config.dbt_cloud_token_value,
        "INPUT_DBT_CLOUD_JOB_ID": mock_config.dbt_cloud_job_id,
        "INPUT_DIALECT": mock_config.dialect,
        "INPUT_DRY_RUN": "true",
    }

    with (
        patch.dict("os.environ", env_vars, clear=True),
        patch("src.config.Config._set_fields_from_dbtc_client"),
    ):
        config_with_dry_run = Config.from_env()
        assert config_with_dry_run.dry_run is True


def test_set_fields_from_dbtc_client_success(mock_config):
    # Mock response from dbt Cloud API
    mock_response = {
        "data": {
            "deferring_environment_id": 218762,
            "project": {"id": 270542, "name": "Main"},
            "execute_steps": ["dbt build -s state:modified+"],
        }
    }

    # Properly mock the get_job method using patch
    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", return_value=mock_response
    ):
        mock_config._set_fields_from_dbtc_client()

        # Assert fields were set correctly from the mock response
        assert mock_config.dbt_cloud_environment_id == 218762
        assert mock_config.dbt_cloud_project_id == 270542
        assert mock_config.dbt_cloud_project_name == "Main"
        assert mock_config.execute_steps == ["dbt build -s state:modified+"]


def test_set_fields_from_dbtc_client_api_error(mock_config):
    # Mock API call to raise an exception
    mock_config.dbtc_client.cloud.get_job = Mock(side_effect=Exception("API Error"))

    with pytest.raises(Exception) as exc_info:
        mock_config._set_fields_from_dbtc_client()

    assert "An error occurred making a request to dbt Cloud" in str(exc_info.value)


def test_set_fields_from_dbtc_client_invalid_response(mock_config):
    # Mock response with missing data
    mock_response = {
        "data": {
            # Missing required fields
        }
    }

    with patch.object(
        mock_config.dbtc_client.cloud, "get_job", return_value=mock_response
    ):
        with pytest.raises(Exception) as exc_info:
            mock_config._set_fields_from_dbtc_client()

    assert "An error occurred retrieving your job's data" in str(exc_info.value)


def test_config_invalid_dialect():
    """Test Config creation with an invalid dialect."""
    env_vars = {
        "INPUT_DBT_CLOUD_HOST": "cloud.getdbt.com",
        "INPUT_DBT_CLOUD_SERVICE_TOKEN": "test_token",
        "INPUT_DBT_CLOUD_TOKEN_NAME": "cloud-cli-6d65",
        "INPUT_DBT_CLOUD_TOKEN_VALUE": "test_token_value",
        "INPUT_DBT_CLOUD_ACCOUNT_ID": "43786",
        "INPUT_DBT_CLOUD_JOB_ID": "567183",
        "INPUT_DIALECT": "invalid_dialect",
    }

    with patch.dict("os.environ", env_vars, clear=True):
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        assert "Invalid dialect: invalid_dialect" in str(exc_info.value)
        assert "Valid dialects are:" in str(exc_info.value)
