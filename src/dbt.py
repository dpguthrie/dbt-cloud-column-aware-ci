# stdlib
import json
import logging
from subprocess import CompletedProcess

# first party
from src.config import Config
from src.discovery_api_queries import QUERIES

logger = logging.getLogger(__name__)


DBT_COMMANDS: dict[str, list[str]] = {
    "compile": [
        "dbt",
        "compile",
        "-s",
        "state:modified,resource_type:model",
        "--favor-state",
    ],
    "ls": [
        "dbt",
        "ls",
        "--resource-type",
        "model",
        "--select",
        "state:modified+",
        "--output",
        "json",
    ],
}


def get_target_compiled_code() -> dict[str, dict[str, str]]:
    logger.info("Parsing run_results for compiled code...")

    with open("target/run_results.json") as rr:
        run_results_json = json.load(rr)

    modified_nodes = {}
    for result in run_results_json.get("results", []):
        relation_name = result["relation_name"]
        if relation_name is not None:
            unique_id = result["unique_id"]
            modified_nodes[unique_id] = {
                "unique_id": unique_id,
                "target_code": result["compiled_code"],
            }
            logger.info(f"Retrieved compiled code for {unique_id}")

    return modified_nodes


def get_source_compiled_code(
    config: Config, unique_ids: list[str]
) -> dict[str, dict[str, str]]:
    variables = {
        "first": 500,
        "after": None,
        "environmentId": config.dbt_cloud_environment_id,
        "filter": {"uniqueIds": unique_ids},
    }

    logger.info("Querying discovery API for compiled code...")

    deferring_env_nodes = config.dbtc_client.metadata.query(
        QUERIES["compiled_code"], variables, paginated_request_to_list=True
    )

    # Error handling
    if deferring_env_nodes and "node" not in deferring_env_nodes[0]:
        logger.error(
            "Error encountered making request to discovery API."
            f"Error message:\n{deferring_env_nodes[0]['message']}\n"
            f"Full response:\n{deferring_env_nodes[0]}"
        )
        return dict()

    modified_nodes: dict[str, dict[str, str]] = {}

    for deferring_env_node in deferring_env_nodes:
        unique_id = deferring_env_node["node"]["uniqueId"]
        compiled_code = deferring_env_node["node"]["compiledCode"]
        modified_nodes[unique_id] = {"source_code": compiled_code}
        logger.info(f"Compiled code found for `{unique_id}`")

    return modified_nodes


def get_all_unique_ids(
    result: CompletedProcess, modified_unique_ids: list[str]
) -> set[str]:
    """Get all unique IDs, including downstream, affected by the PR."""
    all_unique_ids = set()
    for line in result.stdout.split("\n"):
        json_str = line[line.find("{") : line.rfind("}") + 1]
        try:
            data = json.loads(json_str)
            unique_id = data["unique_id"]

            # Only include if it's a downstream node
            if unique_id not in modified_unique_ids:
                all_unique_ids.add(data["unique_id"])

        except ValueError:
            continue

    return all_unique_ids
