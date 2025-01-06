from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch


from src.utils import JobRunStatus, create_dbt_cloud_profile, trigger_job


def test_job_run_status_enum():
    """Test JobRunStatus enum values."""
    assert JobRunStatus.QUEUED == 1
    assert JobRunStatus.STARTING == 2
    assert JobRunStatus.RUNNING == 3
    assert JobRunStatus.SUCCESS == 10
    assert JobRunStatus.ERROR == 20
    assert JobRunStatus.CANCELLED == 30


@patch("pathlib.Path.home")
@patch("pathlib.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
@patch("yaml.dump")
def test_create_dbt_cloud_profile(
    mock_yaml_dump, mock_file, mock_mkdir, mock_home, mock_config
):
    """Test creating dbt Cloud profile."""
    mock_home.return_value = Path("/home/test")

    create_dbt_cloud_profile(mock_config)

    # Verify yaml.dump was called
    mock_yaml_dump.assert_called_once()

    # Get the config dict that was passed to yaml.dump
    config_dict = mock_yaml_dump.call_args[0][0]

    # Your assertions
    assert config_dict["version"] == 1
    assert config_dict["context"]["active-project"] == "270542"
    assert config_dict["projects"][0]["project-id"] == "270542"
    assert (
        config_dict["projects"][0]["token-value"] == mock_config.dbt_cloud_token_value
    )


@patch.dict(
    "os.environ",
    {"GITHUB_HEAD_REF": "feature/test-branch", "GITHUB_REF": "refs/pull/123/merge"},
)
def test_trigger_job_with_exclusions(mock_config):
    """Test triggering job with excluded nodes."""
    excluded_nodes = ["model.test.excluded1", "model.test.excluded2"]

    # Create a Mock for trigger_job that can have return_value set
    mock_trigger = mock_config.dbtc_client.cloud.trigger_job = MagicMock()
    mock_trigger.return_value = {}

    trigger_job(mock_config, excluded_nodes=excluded_nodes)

    mock_trigger.assert_called_once()
    call_args = mock_trigger.call_args[0]

    assert call_args[0] == "43786"  # account_id
    assert call_args[1] == "567183"  # job_id

    payload = call_args[2]
    assert payload["cause"] == "Column-aware CI"
    assert payload["schema_override"] == "dbt_cloud_pr_567183_123"
    assert payload["git_branch"] == "feature/test-branch"
    assert payload["github_pull_request_id"] == 123
    assert payload["steps_override"] == [
        "dbt build -s state:modified+ --exclude model.test.excluded1 model.test.excluded2"
    ]


@patch.dict(
    "os.environ",
    {"GITHUB_HEAD_REF": "feature/test-branch", "GITHUB_REF": "refs/pull/123/merge"},
)
def test_trigger_job_without_exclusions(mock_config):
    """Test triggering job without excluded nodes."""
    # Create a Mock for trigger_job that can have return_value set
    mock_trigger = mock_config.dbtc_client.cloud.trigger_job = MagicMock()
    mock_trigger.return_value = {}

    trigger_job(mock_config)

    mock_trigger.assert_called_once()
    call_args = mock_trigger.call_args[0]

    payload = call_args[2]
    assert "steps_override" not in payload
    assert payload["schema_override"] == "dbt_cloud_pr_567183_123"


@patch.dict(
    "os.environ",
    {"GITHUB_HEAD_REF": "feature/test-branch", "GITHUB_REF": "invalid/ref/format"},
)
def test_trigger_job_invalid_pr_ref(mock_config):
    """Test triggering job with invalid PR reference."""
    # Create a Mock for trigger_job that can have return_value set
    mock_trigger = mock_config.dbtc_client.cloud.trigger_job = MagicMock()
    mock_trigger.return_value = {}

    trigger_job(mock_config)

    payload = mock_trigger.call_args[0][2]
    assert payload["github_pull_request_id"] is None
