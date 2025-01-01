# stdlib
import json
import logging
import subprocess
import typing as t
from dataclasses import dataclass

# third party
from sqlglot import diff, exp, parse_one
from sqlglot.diff import Insert, Move, Remove, Update
from sqlglot.optimizer.scope import Scope, build_scope, find_all_in_scope

# first party
from src.config import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


Edit = t.Union[Insert, Move, Remove, Update]

"""

Take the following scenario (where filter changes):
In [13]: src = 'select 1 as col, 2 as col_2 from x where col > 5'

In [14]: tgt = 'select 1 as col, 2 as col_2 from x where col > 7'

This will create an Insert and Remove breaking change.  Neither will contain a column
name, but this could be a breaking change and would need to be checked for each downstream
model, which wouldn't happen right now.

I would need to manually go and add this to the _impacted_unique_ids set by obtaining
the lineage for the model section syntax of <node.unique_id>+ via the lineage API.
* This could be done in a single call if I have multiple nodes like these with the syntax:

--select <node.unique_id>+ <other_node.unique_id>+ <other_other_node.unique_id>+

** Another scenario**

- If I have both column level changes and whole node level breaking changes, the entire
node level breaking changes should supercede any column level, and we should just assume
that we need to test everything

"""


@dataclass
class BreakingChange:
    edit: Edit

    def __post_init__(self):
        try:
            self._expr = self.edit.expression
        except AttributeError:
            self._expr = self.edit.source

    def _in_cte(self, expr: exp.Expression):
        return expr.find_ancestor(exp.CTE) is not None

    def _find_cte_alias(self, root: Scope, cte: exp.CTE):
        try:
            table_alias = [
                ta
                for ta in root.find_all(exp.TableAlias)
                if ta.find_ancestor(exp.Table).name == cte.alias
            ][0]
            return table_alias.name
        except IndexError:
            return cte.alias

    def _find_parent_column_name(self, expr: t.Union[exp.Column, exp.Alias]):
        # Get the CTE that contains this expression
        cte = expr.find_ancestor(exp.CTE)

        # Build scope for the entire query
        root = build_scope(expr.root())

        # Get the CTE alias and column name from the original expression
        cte_alias = self._find_cte_alias(root, cte)
        column_name = expr.output_name

        # Find all columns in the main SELECT that reference this CTE
        for column in find_all_in_scope(root, exp.Column):
            # Only look at columns in the main SELECT (not in CTEs)
            if not self._in_cte(column):
                # Check if this column references our CTE column
                if column.table == cte_alias and column.name == column_name:
                    # Get the final output name (which may be an alias)
                    parent_alias = column.find_ancestor(exp.Alias)
                    if parent_alias:
                        return parent_alias.output_name
                    return column.output_name

        # If we couldn't find a reference, return the original name
        return column_name

    @property
    def column_name(self) -> t.Union[str, None]:
        expr = self._expr
        while True:
            is_column = expr.key in ["alias", "column"]
            has_ancestor = expr.find_ancestor(exp.Column, exp.Alias) is not None
            if is_column and not has_ancestor:
                # if in CTE, need to find where it's used in case it's a column rename
                if self._in_cte(expr):
                    return self._find_parent_column_name(expr)
                else:
                    return expr.output_name

            elif expr.depth < 1:
                return None

            expr = expr.parent


@dataclass
class Node:
    unique_id: str
    target_code: str
    source_code: str

    def __post_init__(self):
        self._source_exp = parse_one(self.source_code)
        self._target_exp = parse_one(self.target_code)
        try:
            self.changes = diff(self._source_exp, self._target_exp, delta_only=True)
        except Exception as e:
            logger.error(
                f"There was a problem creating a diff for `{self.unique_id}`.\n"
                f"Error: {e}\n\nSource: {self.source_code}\nTarget: {self.target_code}"
            )
            self.changes = []

        # All breaking changes from diff
        self.breaking_changes = self._get_breaking_changes()

        # We should ignore column level changes if there are any node level changes
        self.ignore_column_changes = any(
            bc for bc in self.breaking_changes if bc.column_name is None
        )

        if self.ignore_column_changes:
            self.column_changes = set()
        else:
            self.column_changes = {
                bc.column_name for bc in self.breaking_changes if bc.column_name
            }

    def _get_breaking_changes(self) -> list[BreakingChange]:
        breaking_changes = []

        inserts = {e.expression for e in self.changes if isinstance(e, Insert)}

        for edit in self.changes:
            if not isinstance(edit, Insert):
                breaking_changes.append(BreakingChange(edit))

            elif isinstance(edit.expression, exp.UDTF) or (
                not isinstance(edit.expression.parent, exp.Select)
                and edit.expression.parent not in inserts
            ):
                breaking_changes.append(BreakingChange(edit))

        return breaking_changes


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
        cmd = [
            "dbt",
            "compile",
            "-s",
            "state:modified,resource_type:model",
            "--favor-state",
        ]

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

    def _get_impacted_unique_ids_for_node_columns(self, node: Node) -> set[str]:
        impacted_unique_ids = set()

        # Column level changes
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

    def _get_impacted_unique_ids_for_nodes(self, nodes: list[Node]) -> set[str]:
        query = """
        query Environment($environmentId: BigInt!, $filter: LineageFilter!) {
            environment(id: $environmentId) {
                applied {
            lineage(filter: $filter) {
                uniqueId
            }
            }
        }
        """
        names = [node.unique_id.split(".")[-1] for node in nodes]
        names_str = "+ ".join(names) + "+"
        variables = {
            "environmentId": self.config.dbt_cloud_environment_id,
            "filter": {
                "select": f"--select {names_str}",
                "exclude": f"--exclude {' '.join(names)}",
                # Only getting models for now
                "types": ["Model"],
            },
        }
        results = self.config.dbtc_client.metadata.query(query, variables)
        try:
            return {
                r["uniqueId"]
                for r in results["data"]["environment"]["applied"]["lineage"]
            }
        except Exception as e:
            logger.error(f"Error occurred retrieving lineage for {names_str}:\n{e}")
            return set()

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

        # Column level changes
        for node in self.nodes:
            if node.column_changes:
                self._all_impacted_unique_ids.update(
                    self._get_impacted_unique_ids_for_node_column(node)
                )

        # Node level changes
        nodes = [node for node in self.nodes if not node.ignore_column_changes]
        if nodes:
            self._all_impacted_unique_ids.update(
                self._get_impacted_unique_ids_for_nodes(nodes)
            )

        excluded_nodes = self._all_unique_ids - self._all_impacted_unique_ids
        return [em.split(".")[-1] for em in excluded_nodes]
