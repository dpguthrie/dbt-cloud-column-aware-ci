# stdlib
import os
from dataclasses import dataclass

# third party
from dbtc import dbtCloudClient


@dataclass
class Config:
    dbt_cloud_host: str = None
    dbt_cloud_project_id: str = None
    dbt_cloud_project_name: str = None
    dbt_cloud_token_name: str = None
    dbt_cloud_token_value: str = None
    dbt_cloud_account_id: str = None
    dbt_cloud_job_id: str = None
    dbt_cloud_environment_id: str = None
    dbtc_client: dbtCloudClient = None

    def __post_init__(self):
        self._set_dbt_cloud_attributes()
        self.dbtc_client = dbtCloudClient(host=self.dbt_cloud_host)
        if not hasattr(self, "dbt_cloud_environment_id"):
            self._set_deferring_environment_id()

    def _set_dbt_cloud_attributes(self) -> None:
        for env_var in os.environ:
            if env_var.startswith("INPUT_DBT_CLOUD_"):
                name = env_var.replace("INPUT_", "").lower()
                value = os.environ[env_var]
                setattr(self, name, value)

    def _set_deferring_environment_id(self):
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
