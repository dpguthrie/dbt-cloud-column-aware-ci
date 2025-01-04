# stdlib
import enum
import os
import pathlib
import re

# third party
import yaml

# first party
from src.config import Config


class JobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


def create_dbt_cloud_profile(config: Config) -> None:
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


def trigger_job(config: Config, *, excluded_nodes: list[str] = None) -> dict:
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
    # https://docs.getdbt.com/docs/deploy/ci-jobs#trigger-a-ci-job-with-the-api
    payload = {
        "cause": "Column-aware CI",
        "schema_override": schema_override,
        "git_branch": GITHUB_BRANCH,
        "github_pull_request_id": pull_request_id,
    }

    if excluded_nodes:
        excluded_nodes_str = " ".join(excluded_nodes)
        steps_override = [
            f"dbt build -s state:modified+ --exclude {excluded_nodes_str}"
        ]
        payload["steps_override"] = steps_override

    return config.dbtc_client.cloud.trigger_job(
        config.dbt_cloud_account_id, config.dbt_cloud_job_id, payload, should_poll=True
    )
