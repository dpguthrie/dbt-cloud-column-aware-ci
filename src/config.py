# stdlib
import logging
import os
from dataclasses import dataclass, field

# third party
from dbtc import dbtCloudClient

logger = logging.getLogger(__name__)

VALID_DIALECTS = (
    "athena",
    "bigquery", 
    "databricks",
    "postgres",
    "redshift",
    "snowflake",
    "spark",
    "trino"
)


@dataclass
class Config:
    # All fields are required and populated from INPUT_DBT_CLOUD_* environment variables
    dbt_cloud_host: str
    dbt_cloud_service_token: str
    dbt_cloud_token_name: str
    dbt_cloud_token_value: str
    dbt_cloud_account_id: int
    dbt_cloud_job_id: int
    dialect: str

    # Optional fields
    dry_run: bool = field(default=False)

    # Set in post_init, used to find fields below
    dbtc_client: dbtCloudClient = field(init=False)
    # Found from dbtc_client.cloud.get_job
    dbt_cloud_project_id: int = field(default=None)
    dbt_cloud_project_name: int = field(default=None)
    dbt_cloud_environment_id: int = field(default=None)
    execute_steps: list[str] = field(init=False, default_factory=list)

    def __post_init__(self):
        self.dbtc_client = dbtCloudClient(
            service_token=self.dbt_cloud_service_token, host=self.dbt_cloud_host
        )
        self._set_fields_from_dbtc_client()

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
            if dialect.lower() not in VALID_DIALECTS:
                raise ValueError(
                    f"Invalid dialect: {dialect}. Valid dialects are: {VALID_DIALECTS}"
                )
            env_vars["dialect"] = dialect.lower()

        dry_run = os.getenv("INPUT_DRY_RUN", "false").lower() == "true"
        env_vars["dry_run"] = dry_run

        missing_vars = []
        required_vars = [
            "dbt_cloud_host",
            "dbt_cloud_service_token",
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

    def _set_fields_from_dbtc_client(self) -> None:
        try:
            ci_job = self.dbtc_client.cloud.get_job(
                self.dbt_cloud_account_id,
                self.dbt_cloud_job_id,
                include_related=["project"],
            )
        except Exception as e:
            raise Exception(
                f"An error occurred making a request to dbt Cloud. See error: {e}"
            )
        try:
            job_data = ci_job["data"]
            self.dbt_cloud_environment_id = job_data["deferring_environment_id"]
            self.dbt_cloud_project_id = job_data["project"]["id"]
            self.dbt_cloud_project_name = job_data["project"]["name"]
            self.execute_steps = job_data["execute_steps"]
        except Exception as e:
            raise Exception(
                "An error occurred retrieving your job's data. "
                f"Response from API:\n{ci_job}.\nError:\n{e}"
            )
