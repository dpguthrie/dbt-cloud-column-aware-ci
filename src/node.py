# stdlib
import logging
import typing as t
from dataclasses import dataclass

# third party
from sqlglot import diff, exp, parse_one
from sqlglot.diff import Insert, Move, Remove, Update
from sqlglot.optimizer.scope import Scope, build_scope, find_all_in_scope

# first party
from src.config import Config
from src.discovery_api_queries import QUERIES

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


Edit = t.Union[Insert, Move, Remove, Update]


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
    def __init__(
        self,
        config: Config,
        all_nodes: dict[str, dict[str, str]],
        all_unique_ids: set[str],
    ):
        self.config = config
        self._node_dict = {
            k: Node(**v) for k, v in all_nodes.items() if v.get("source_code")
        }
        self.all_unique_ids = all_unique_ids
        self._all_impacted_unique_ids: set[str] = set()
        self._node_column_set: set[str] = set()

    @property
    def node_unique_ids(self) -> list[str]:
        return list(self._node_dict.keys())

    @property
    def nodes(self) -> list[Node]:
        return list(self._node_dict.values())

    def _get_impacted_unique_ids_for_node_columns(self, node: Node) -> set[str]:
        impacted_unique_ids = set()

        # Column level changes
        for column_name in node.column_changes:
            node_column = f"{node.unique_id}.{column_name}"
            if node_column not in self._node_column_set:
                logger.info(
                    f"Column `{column_name}` in node `{node.unique_id}` "
                    f"has a change.  Finding downstream nodes using this column ..."
                )
                impacted_unique_ids.update(
                    self._get_downstream_nodes_from_column(node.unique_id, column_name)
                )
                self._node_column_set.add(node_column)

        return impacted_unique_ids

    def _get_downstream_nodes_from_column(
        self, unique_id: str, column_name: str
    ) -> set[str]:
        variables = {
            "environmentId": self.config.dbt_cloud_environment_id,
            "nodeUniqueId": unique_id,
            # TODO - Snowflake returns column names as uppercase, so that's what we have
            "filters": {"columnName": column_name.upper()},
        }
        results = self.config.dbtc_client.metadata.query(
            QUERIES["column_lineage"], variables
        )
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

        if downstream_nodes:
            logger.info(
                f"Column `{column_name}` in node `{unique_id}` is being used by the "
                f"following downstream nodes: `{', '.join(downstream_nodes)}"
            )
        else:
            logger.info(
                f"Column `{column_name}` in node `{unique_id}` is NOT being used "
                "anywhere downstream."
            )

        return downstream_nodes

    def _get_impacted_unique_ids_for_nodes(self, nodes: list[Node]) -> set[str]:
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
        results = self.config.dbtc_client.metadata.query(
            QUERIES["node_lineage"], variables
        )
        logger.info(f"Results:\n{results}")
        try:
            return {
                r["uniqueId"]
                for r in results["data"]["environment"]["applied"]["lineage"]
            }
        except Exception:
            logger.error(f"Error occurred retrieving lineage for {names_str}")
            logger.error(f"Response:\n{results}")
            return set()

    def get_excluded_nodes(self) -> list[str]:
        if not self.nodes:
            return list()

        if not self.all_unique_ids:
            return list()

        # Column level changes
        for node in self.nodes:
            if node.column_changes:
                self._all_impacted_unique_ids.update(
                    self._get_impacted_unique_ids_for_node_columns(node)
                )

        # Node level changes
        nodes = [node for node in self.nodes if node.ignore_column_changes]
        if nodes:
            logger.info("Some nodes were found to have node level breaking changes...")
            logger.info(f"Nodes: {', '.join([n.unique_id for n in nodes])}")
            self._all_impacted_unique_ids.update(
                self._get_impacted_unique_ids_for_nodes(nodes)
            )

        excluded_nodes = self.all_unique_ids - self._all_impacted_unique_ids
        return [em.split(".")[-1] for em in excluded_nodes]
