# stdlib
import logging
import os
from dataclasses import dataclass, field

# third party
from dbtc import dbtCloudClient

logger = logging.getLogger(__name__)


@dataclass
class Config:
    # All fields are required and populated from INPUT_DBT_CLOUD_* environment variables
    dbt_cloud_host: str
    dbt_cloud_service_token: str
    dbt_cloud_project_id: str
    dbt_cloud_project_name: str
    dbt_cloud_token_name: str
    dbt_cloud_token_value: str
    dbt_cloud_account_id: str
    dbt_cloud_job_id: str
    dialect: str
    dbt_cloud_environment_id: str = field(default=None)
    dbtc_client: dbtCloudClient = field(
        init=False
    )  # This is set in post_init, so we'll keep it as a field

    def __post_init__(self):
        self.dbtc_client = dbtCloudClient(
            service_token=self.dbt_cloud_service_token, host=self.dbt_cloud_host
        )
        if self.dbt_cloud_environment_id is None:
            self._set_deferring_environment_id()

    @classmethod
    def from_env(cls) -> "Config":
        """Create a Config instance from environment variables."""

        def is_valid_field(cls, field_name: str) -> bool:
            return field_name in cls.__dataclass_fields__

        env_vars = {}
        for env_var in os.environ:
            if env_var.startswith("INPUT_DBT_CLOUD_"):
                name = env_var.replace("INPUT_", "").lower()
                if is_valid_field(cls, name):
                    env_vars[name] = os.environ[env_var]
                else:
                    logger.warning(
                        f"Ignoring invalid field name found in environment: {name}"
                    )

        dialect = os.getenv("INPUT_DIALECT", None)
        if dialect is not None:
            env_vars["dialect"] = dialect

        missing_vars = []
        required_vars = [
            "dbt_cloud_host",
            "dbt_cloud_service_token",
            "dbt_cloud_project_id",
            "dbt_cloud_project_name",
            "dbt_cloud_token_name",
            "dbt_cloud_token_value",
            "dbt_cloud_account_id",
            "dbt_cloud_job_id",
            "dialect",
        ]

        for var in required_vars:
            if var not in env_vars:
                missing_vars.append(f"INPUT_{var.upper()}")

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return cls(**env_vars)

    def _set_deferring_environment_id(self) -> None:
        try:
            ci_job = self.dbtc_client.cloud.get_job(
                self.dbt_cloud_account_id, self.dbt_cloud_job_id
            )
        except Exception as e:
            raise Exception(
                "An error occurred making a request to dbt Cloud." f"See error: {e}"
            )
        try:
            self.dbt_cloud_environment_id = ci_job["data"]["deferring_environment_id"]
        except Exception as e:
            raise Exception(
                "An error occurred retrieving your job's deferring environment ID. "
                f"Response from API:\n{ci_job}.\nError:\n{e}"
            )
