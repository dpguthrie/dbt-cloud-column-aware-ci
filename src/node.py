# stdlib
import json
import logging
import subprocess
from dataclasses import dataclass

# third party
from sqlglot import diff, parse_one
from sqlglot.diff import Insert, Move, Remove, Update
from sqlglot.expressions import Column

# first party
from src.config import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class Node:
    unique_id: str
    target_code: str
    source_code: str

    def __post_init__(self):
        try:
            self.diff = diff(parse_one(self.source_code), parse_one(self.target_code))
        except Exception as e:
            logger.error(
                f"There was a problem creating a diff for {self.unique_id}.\n"
                f"Error: {e}\n\nSource: {self.source_code}\nTarget: {self.target_code}"
            )
            self.diff = []

    @property
    def valid_changes(self):
        def is_valid_change(change: Insert | Move | Remove | Update) -> bool:
            return change.__class__ not in [Move]

        return [change for change in self.diff if is_valid_change(change)]

    @property
    def column_changes(self) -> set[str]:
        """Returns a set containing column names for all changed columns."""
        changed_columns = set()
        for change in self.valid_changes:
            if hasattr(change, "expression"):
                expression = change.expression
                while True:
                    column = expression.find(Column)
                    if column is not None:
                        changed_columns.add(column.name)
                        break
                    elif expression.depth < 1:
                        break
                    expression = expression.parent
        return changed_columns


class NodeManager:
    def __init__(self, config: Config):
        self.config = config
        self._node_dict: dict[str, Node] = {}
        self._all_unique_ids: set[str] = set()
        self._all_impacted_unique_ids: set[str] = set()
        self._node_column_set: set[str] = set()
        self.set_node_dict()

    @property
    def node_unique_ids(self) -> list[str]:
        return list(self._node_dict.keys())

    @property
    def nodes(self) -> list[Node]:
        return list(self._node_dict.values())

    def set_node_dict(self) -> None:
        modified_nodes = self._get_target_code()
        if modified_nodes:
            modified_nodes = self._get_source_code(modified_nodes)

        self._node_dict = {
            k: Node(**v) for k, v in modified_nodes.items() if v.get("source_code")
        }

    def _get_target_code(self) -> dict[str, dict[str, str]]:
        cmd = ["dbt", "compile", "-s", "state:modified,resource_type:model"]

        logger.info("Compiling code for any modified nodes...")

        _ = subprocess.run(cmd, capture_output=True)

        # This will generate a run_results.json file, among other things, which will
        # contain the compiled code for each node

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

    def _get_source_code(
        self, modified_nodes: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        query = """
        query Environment($environmentId: BigInt!, $filter: ModelAppliedFilter, $first: Int, $after: String) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $filter, first: $first, after: $after) {
                        edges {
                            node {
                                compiledCode
                                uniqueId
                            }
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                            hasPreviousPage
                            startCursor
                        }
                        totalCount
                    }
                }
            }
        }
        """

        variables = {
            "first": 500,
            "after": None,
            "environmentId": self.config.dbt_cloud_environment_id,
            "filter": {"uniqueIds": self.node_unique_ids},
        }

        logger.info("Querying discovery API for compiled code...")

        deferring_env_nodes = self.config.dbtc_client.metadata.query(
            query, variables, paginated_request_to_list=True
        )

        # Error handling
        if deferring_env_nodes and "node" not in deferring_env_nodes[0]:
            logger.error(
                "Error encountered making request to discovery API."
                f"Error message:\n{deferring_env_nodes[0]['message']}\n"
                f"Full response:\n{deferring_env_nodes[0]}"
            )
            raise

        for deferring_env_node in deferring_env_nodes:
            unique_id = deferring_env_node["node"]["uniqueId"]
            if unique_id in modified_nodes.keys():
                logger.info(f"Compiled source code found for `{unique_id}`")
                modified_nodes[unique_id]["source_code"] = deferring_env_node["node"][
                    "compiledCode"
                ]

        return modified_nodes

    def _get_impacted_unique_ids_for_node(self, node: Node) -> set[str]:
        impacted_unique_ids = set()
        for column_name in node.column_changes:
            node_column = f"{node.unique_id}.{column_name}"
            if node_column not in self._node_column_set:
                logger.info(
                    f"Column `{column_name}` in node `{node.unique_id}` "
                    f"has a change.\n"
                    "Finding downstream nodes using this column ..."
                )
                impacted_unique_ids.update(
                    self._get_downstream_nodes_from_column(node.unique_id, column_name)
                )
                self._node_column_set.add(node_column)

        return impacted_unique_ids

    def _get_all_unique_ids(self) -> set[str]:
        """Get all downstream unique IDs using dbt ls"""
        cmd = [
            "dbt",
            "ls",
            "--resource-type",
            "model",
            "--select",
            "state:modified+",
            "--output",
            "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        self._all_unique_ids = set()
        for line in result.stdout.split("\n"):
            json_str = line[line.find("{") : line.rfind("}") + 1]
            try:
                data = json.loads(json_str)
                unique_id = data["unique_id"]

                # Only include if it's a downstream node
                if unique_id not in self.node_unique_ids:
                    self._all_unique_ids.add(data["unique_id"])
            except ValueError:
                continue

    def _get_downstream_nodes_from_column(
        self, unique_id: str, column_name: str
    ) -> set[str]:
        query = """
        query Column($environmentId: BigInt!, $nodeUniqueId: String!, $filters: ColumnLineageFilter) {
            column(environmentId: $environmentId) {
                lineage(nodeUniqueId: $nodeUniqueId, filters: $filters) {
                    nodeUniqueId
                    relationship
                }
            }
        }
        """
        variables = {
            "environmentId": self.config.dbt_cloud_environment_id,
            "nodeUniqueId": unique_id,
            # TODO - Snowflake returns column names as uppercase, so that's what we have
            "filters": {"columnName": column_name.upper()},
        }
        results = self.config.dbtc_client.metadata.query(query, variables)
        try:
            lineage = results["data"]["column"]["lineage"]
        except Exception as e:
            logger.error(
                f"Error occurred retrieving column lineage for {column_name}"
                f"in {unique_id}:\n{e}"
            )
            return set()

        downstream_nodes = set()
        for node in lineage:
            if node["relationship"] == "child":
                downstream_nodes.add(node["nodeUniqueId"])

        return downstream_nodes

    def get_excluded_nodes(self) -> list[str]:
        if not self.nodes:
            return list()

        self._get_all_unique_ids()

        if not self._all_unique_ids:
            return list()

        for node in self.nodes:
            if node.valid_changes:
                self._all_impacted_unique_ids.update(
                    self._get_impacted_unique_ids_for_node(node)
                )

        excluded_nodes = self._all_unique_ids - self._all_impacted_unique_ids
        return [em.split(".")[-1] for em in excluded_nodes]
