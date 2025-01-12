# stdlib
import enum
import logging
import os
import pathlib
import re

# third party
import requests
import yaml

# first party
from src.config import Config

logger = logging.getLogger(__name__)


class JobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


def create_dbt_cloud_profile(config: Config) -> None:
    """Create dbt Cloud profile for authentication."""
    logger.debug("Creating dbt Cloud profile")

    dbt_cloud_config = {
        "version": 1,
        "context": {
            "active-project": config.dbt_cloud_project_id,
            "active-host": config.dbt_cloud_host,
        },
        "projects": [
            {
                "project-name": config.dbt_cloud_project_name,
                "project-id": config.dbt_cloud_project_id,
                "account-host": config.dbt_cloud_host,
                "token-name": config.dbt_cloud_token_name,
                "token-value": config.dbt_cloud_token_value,
            }
        ],
    }

    dbt_dir = pathlib.Path.home() / ".dbt"
    dbt_dir.mkdir(parents=True, exist_ok=True)

    config_path = dbt_dir / "dbt_cloud.yml"
    with open(config_path, "w") as f:
        yaml.dump(dbt_cloud_config, f)

    logger.info(
        "Successfully created dbt Cloud profile", extra={"path": str(config_path)}
    )


def is_valid_command(command: str) -> bool:
    dbt_flags = [
        "--warn-error",
        "--use-experimental-parser",
        "--no-partial-parse",
        "--fail-fast",
    ]

    dbt_commands = [
        "run",
        "test",
        # "snapshot",
        "source",
        "compile",
        "ls",
        "list",
        r"docs\s+generate",
        "build",
        "clone",
    ]

    valid_commands = r"\s*dbt\s+(({})\s+)*({})\s*.*".format(
        "|".join(dbt_flags), "|".join(dbt_commands)
    )

    return re.match(valid_commands, command) is not None


def trigger_job(config: Config, *, excluded_nodes: list[str] = None) -> dict:
    def modify_execute_steps(
        execute_steps: list[str], excluded_nodes: list[str]
    ) -> list[str]:
        """Modify the execute steps to include node exclusions."""
        excluded_nodes_str = " ".join(excluded_nodes)
        new_steps = []
        for step in execute_steps:
            if is_valid_command(step):
                new_steps.append(f"{step} --exclude {excluded_nodes_str}")
            else:
                new_steps.append(step)
        return new_steps

    """Trigger a dbt Cloud job with optional node exclusions."""
    logger.debug(
        "Preparing to trigger dbt Cloud job",
        extra={
            "job_id": config.dbt_cloud_job_id,
            "has_exclusions": bool(excluded_nodes),
        },
    )

    def extract_pr_number(s):
        match = re.search(r"refs/pull/(\d+)/merge", s)
        return int(match.group(1)) if match else None

    GITHUB_BRANCH = os.environ["GITHUB_HEAD_REF"]
    GITHUB_REF = os.environ["GITHUB_REF"]

    # Extract PR Number
    pull_request_id = extract_pr_number(GITHUB_REF)

    # Create schema
    schema_override = f"dbt_cloud_pr_{config.dbt_cloud_job_id}_{pull_request_id}"

    # Create payload to pass to job
    payload = {
        "cause": "Column-aware CI",
        "schema_override": schema_override,
        "git_branch": GITHUB_BRANCH,
        "github_pull_request_id": pull_request_id,
    }

    if excluded_nodes:
        payload["steps_override"] = modify_execute_steps(
            config.execute_steps, excluded_nodes
        )
        logger.info(
            "Adding node exclusions to job",
            extra={"excluded_count": len(excluded_nodes)},
        )

    logger.info(
        "Triggering dbt Cloud job",
        extra={
            "job_id": config.dbt_cloud_job_id,
            "schema": schema_override,
            "pr_id": pull_request_id,
        },
    )

    return config.dbtc_client.cloud.trigger_job(
        config.dbt_cloud_account_id, config.dbt_cloud_job_id, payload, should_poll=True
    )


def post_dry_run_message(excluded_nodes: list[str]) -> None:
    def extract_pr_number(s):
        match = re.search(r"refs/pull/(\d+)/merge", s)
        return int(match.group(1)) if match else None

    """Post a message to the console indicating that the job would have been run with the given exclusions."""
    # Convert None to empty list and ensure proper markdown formatting
    nodes_list = sorted(excluded_nodes or [])
    nodes_markdown = (
        "\n".join([f"* {node}" for node in nodes_list])
        if nodes_list
        else "_No models excluded_"
    )

    dry_run_message = (
        "## Column-aware CI Results (dry run)\n\n"
        "The total number of models that would've been excluded from the build "
        f"are: {len(excluded_nodes or [])}"
        "\n<details>"
        "<summary>Models that would've been excluded from the build are listed below:</summary>\n\n"
        f"{nodes_markdown}"
        "\n</details>"
    )
    logger.info(dry_run_message)

    required_env_vars = {
        "token": os.getenv("INPUT_GITHUB_TOKEN", None),
        "repository": os.getenv("GITHUB_REPOSITORY", None),
        "pull_request_id": extract_pr_number(os.getenv("GITHUB_REF", "")),
    }

    if all(required_env_vars.values()):
        url = f"https://api.github.com/repos/{required_env_vars['repository']}/issues/{required_env_vars['pull_request_id']}/comments"
        headers = {
            "Authorization": f"Bearer {required_env_vars['token']}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.post(url, headers=headers, json={"body": dry_run_message})
        if response.ok:
            logger.info("Successfully posted dry run message to GitHub")
        else:
            logger.error("Failed to post dry run message to GitHub")

    else:
        missing_vars = [k for k, v in required_env_vars.items() if not v]
        logger.warning(
            "Missing required environment variables for GitHub comment: "
            f"{', '.join(missing_vars)}"
        )
