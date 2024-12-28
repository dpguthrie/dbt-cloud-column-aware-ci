# stdlib
import os
import sys

# first party
from src.config import Config
from src.node import NodeManager
from src.utils import JobRunStatus, create_dbt_cloud_profile, trigger_job

if __name__ == "__main__":
    config = Config()
    create_dbt_cloud_profile(config)
    node_manager = NodeManager(config)
    excluded_nodes = node_manager.get_excluded_nodes()
    run = trigger_job(config, excluded_nodes=excluded_nodes)
    try:
        run_status = run["status"]
    except Exception:
        sys.exit(1)

    if run_status in (JobRunStatus.ERROR, JobRunStatus.CANCELLED):
        sys.exit(1)

    sys.exit(0)
