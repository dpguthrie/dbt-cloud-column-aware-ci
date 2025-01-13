from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from src.utils import (
    JobRunStatus,
    create_dbt_cloud_profile,
    is_valid_command,
    post_dry_run_message,
    trigger_job,
)


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
    assert config_dict["context"]["active-project"] == 270542
    assert config_dict["projects"][0]["project-id"] == 270542
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


@patch("requests.post")
@patch.dict(
    "os.environ",
    {
        "INPUT_GITHUB_TOKEN": "fake-token",
        "GITHUB_REPOSITORY": "org/repo",
        "GITHUB_REF": "refs/pull/123/merge",
    },
)
def test_post_dry_run_message_success(mock_post):
    """Test posting dry run message with successful GitHub API call."""
    # Setup mock response
    mock_post.return_value.ok = True

    excluded_nodes = ["excluded1", "excluded2"]
    post_dry_run_message(excluded_nodes)

    # Verify the API call
    mock_post.assert_called_once()
    call_args = mock_post.call_args

    # Check URL
    assert (
        call_args[0][0] == "https://api.github.com/repos/org/repo/issues/123/comments"
    )

    # Check headers
    assert call_args[1]["headers"] == {
        "Authorization": "Bearer fake-token",
        "Accept": "application/vnd.github.v3+json",
    }

    # Check message content
    message = call_args[1]["json"]["body"]
    assert "Column-aware CI Results (dry run)" in message
    assert "number of models that would've been excluded" in message
    assert "excluded1" in message
    assert "excluded2" in message
    assert "2" in message  # Number of excluded models


@patch("requests.post")
@patch.dict(
    "os.environ",
    {
        "GITHUB_REPOSITORY": "org/repo",
        "GITHUB_REF": "refs/pull/123/merge",
        # Intentionally missing GITHUB_TOKEN
    },
)
def test_post_dry_run_message_missing_env_vars(mock_post, caplog):
    """Test posting dry run message with missing environment variables."""
    excluded_nodes = ["excluded1"]
    post_dry_run_message(excluded_nodes)

    # Verify no API call was made
    mock_post.assert_not_called()

    # Verify log message
    assert (
        "Missing required environment variables for GitHub comment: token"
        in caplog.text
    )


@patch.dict(
    "os.environ",
    {"GITHUB_HEAD_REF": "feature/test-branch", "GITHUB_REF": "refs/pull/123/merge"},
)
def test_trigger_job_mixed_execute_steps(mock_config):
    """Test triggering job with mixed execute steps (some eligible for exclusions, some not)."""
    excluded_nodes = ["model.test.excluded1", "model.test.excluded2"]

    # Set up mixed execute steps in the mock config
    mock_config.execute_steps = [
        "dbt deps",  # Not eligible for exclusions
        "dbt build -s state:modified+",  # Should get exclusions
        "dbt test -s source:*",  # Should get exclusions
        "dbt docs generate",  # Should get exclusions
        "dbt run --select tag:daily",  # Should get exclusions
        "dbt seed",  # Not eligible for exclusions
    ]

    # Create a Mock for trigger_job
    mock_trigger = mock_config.dbtc_client.cloud.trigger_job = MagicMock()
    mock_trigger.return_value = {}

    trigger_job(mock_config, excluded_nodes=excluded_nodes)

    mock_trigger.assert_called_once()
    payload = mock_trigger.call_args[0][2]

    # Verify the steps were modified correctly
    expected_steps = [
        "dbt deps",  # Unchanged
        "dbt build -s state:modified+ --exclude model.test.excluded1 model.test.excluded2",
        "dbt test -s source:* --exclude model.test.excluded1 model.test.excluded2",
        "dbt docs generate --exclude model.test.excluded1 model.test.excluded2",
        "dbt run --select tag:daily --exclude model.test.excluded1 model.test.excluded2",
        "dbt seed",  # Unchanged
    ]
    assert payload["steps_override"] == expected_steps


@patch.dict(
    "os.environ",
    {"GITHUB_HEAD_REF": "feature/test-branch", "GITHUB_REF": "refs/pull/123/merge"},
)
def test_trigger_job_with_existing_exclusions(mock_config):
    """Test triggering job with steps that already have exclusions."""
    excluded_nodes = ["model.test.new_exclude1", "model.test.new_exclude2"]

    # Set up execute steps that already have exclusions
    mock_config.execute_steps = [
        "dbt deps",  # Not eligible for exclusions
        "dbt build -s state:modified+ --exclude model.test.existing1",  # Has existing exclusion
        "dbt test --exclude model.test.existing2 model.test.existing3",  # Has multiple existing exclusions
        "dbt run",  # No existing exclusions
    ]

    # Create a Mock for trigger_job
    mock_trigger = mock_config.dbtc_client.cloud.trigger_job = MagicMock()
    mock_trigger.return_value = {}

    trigger_job(mock_config, excluded_nodes=excluded_nodes)

    mock_trigger.assert_called_once()
    payload = mock_trigger.call_args[0][2]

    # Verify the steps were modified correctly
    expected_steps = [
        "dbt deps",  # Unchanged
        "dbt build -s state:modified+ --exclude model.test.existing1 --exclude model.test.new_exclude1 model.test.new_exclude2",
        "dbt test --exclude model.test.existing2 model.test.existing3 --exclude model.test.new_exclude1 model.test.new_exclude2",
        "dbt run --exclude model.test.new_exclude1 model.test.new_exclude2",
    ]
    assert payload["steps_override"] == expected_steps


def test_is_valid_command():
    """Test validation of dbt commands."""
    # Valid commands
    assert is_valid_command("dbt run")
    assert is_valid_command("dbt test")
    assert is_valid_command("dbt build")
    assert is_valid_command("dbt docs generate")
    assert is_valid_command("dbt source freshness")
    assert is_valid_command("  dbt   run  ")  # Extra spaces

    # Valid commands with flags
    assert is_valid_command("dbt --warn-error run")
    assert is_valid_command("dbt --fail-fast test")
    assert is_valid_command("dbt --use-experimental-parser --no-partial-parse run")
    assert is_valid_command(
        "dbt run --select tag:daily"
    )  # Additional arguments after command

    # Invalid commands
    assert not is_valid_command("dbt snapshot")  # Commented out in allowed commands
    assert not is_valid_command("dbt invalid")
    assert not is_valid_command("not-dbt run")
    assert not is_valid_command("dbt")  # Missing command
    assert not is_valid_command("")  # Empty string
