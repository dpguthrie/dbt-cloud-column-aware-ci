# stdlib
import typing as t
from dataclasses import dataclass

# third party
from sqlglot import exp
from sqlglot.diff import Insert, Move, Remove, Update
from sqlglot.optimizer.scope import Scope, build_scope, find_all_in_scope

Edit = t.Union[Insert, Move, Remove, Update]


@dataclass
class BreakingChange:
    """
    Represents a breaking change in SQL code.

    A breaking change is any modification that could potentially affect downstream
    dependencies. This includes column removals, renames, or structural changes.

    Attributes:
        edit: The SQL edit operation that represents the breaking change
    """

    edit: Edit
    _expr: t.Union[exp.Expression, None] = None

    def __post_init__(self) -> None:
        """Initialize the expression from the edit operation."""
        try:
            self._expr = self.edit.expression
        except AttributeError:
            self._expr = self.edit.source

    def _in_cte(self, expr: exp.Expression) -> bool:
        """
        Check if an expression is within a CTE.

        Args:
            expr: The SQL expression to check

        Returns:
            bool: True if the expression is within a CTE, False otherwise
        """
        return expr.find_ancestor(exp.CTE) is not None

    def _find_cte_alias(self, root: Scope, cte: exp.CTE) -> str:
        """
        Find the alias used for a CTE in the main query.

        Args:
            root: The root scope of the SQL query
            cte: The CTE expression to find the alias for

        Returns:
            str: The alias used for the CTE in the main query
        """
        try:
            table_alias = [
                ta
                for ta in root.find_all(exp.TableAlias)
                if ta.find_ancestor(exp.Table).name == cte.alias
            ][0]
            return table_alias.name
        except IndexError:
            return cte.alias

    def _find_parent_column_name(self, expr: t.Union[exp.Column, exp.Alias]) -> str:
        """
        Find the final output name of a column in a CTE.

        This method traces a column through CTEs to find its final output name
        in the main query.

        Args:
            expr: The column or alias expression to trace

        Returns:
            str: The final output name of the column
        """
        # Get the CTE that contains this expression
        cte = expr.find_ancestor(exp.CTE)

        # Build scope for the entire query
        root = build_scope(expr.root())

        # Get the CTE alias and column name from the original expression
        cte_alias = self._find_cte_alias(root, cte)
        column_name = expr.alias_or_name

        # Find all columns in the main SELECT that reference this CTE
        for column in find_all_in_scope(root, exp.Column):
            # Only look at columns in the main SELECT (not in CTEs)
            if not self._in_cte(column):
                # Check if this column references our CTE column
                if column.table == cte_alias and column.name == column_name:
                    # Get the final output name (which may be an alias)
                    parent_alias = column.find_ancestor(exp.Alias)
                    if parent_alias:
                        return parent_alias.alias_or_name
                    return column.alias_or_name

        # If we couldn't find a reference, return the original name
        return column_name

    @property
    def column_name(self) -> t.Optional[str]:
        """
        Get the name of the column affected by this breaking change.

        Returns:
            Optional[str]: The name of the affected column, or None if the change
                         is not column-specific
        """
        expr = self._expr
        while True:
            is_column = expr.key in ["alias", "column"]
            has_ancestor = expr.find_ancestor(exp.Column, exp.Alias) is not None
            if is_column and not has_ancestor:
                # if in CTE, need to find where it's used in case it's a column rename
                if self._in_cte(expr):
                    return self._find_parent_column_name(expr)
                else:
                    return expr.alias_or_name

            elif expr.depth < 1:
                return None

            expr = expr.parent
