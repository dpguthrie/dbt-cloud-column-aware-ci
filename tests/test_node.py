import unittest

from src.node import Node


class TestNode(unittest.TestCase):
    def test_simple_column_rename(self):
        """Test when a column is renamed - should be a breaking change"""
        source = "SELECT amount as revenue, id FROM sales"
        target = "SELECT amount as income, id FROM sales"

        node = Node(
            unique_id="model.test.simple_rename", target_code=target, source_code=source
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"revenue"})

    def test_column_removal(self):
        """Test when a column is removed - should be a breaking change"""
        source = "SELECT id, name, email, phone FROM users"
        target = "SELECT id, name, email FROM users"

        node = Node(
            unique_id="model.test.remove_column", target_code=target, source_code=source
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"phone"})

    def test_column_addition(self):
        """Test when a column is added - should not be a breaking change"""
        source = "SELECT id, name FROM customers"
        target = "SELECT id, name, address FROM customers"

        node = Node(
            unique_id="model.test.add_column", target_code=target, source_code=source
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, set())

    def test_column_expression_change(self):
        """Test when a column's expression changes - should be a breaking change"""
        source = "SELECT id, price * quantity as total FROM orders"
        target = "SELECT id, price * quantity * (1 - discount) as total FROM orders"

        node = Node(
            unique_id="model.test.expression_change",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"total"})

    def test_table_source_change(self):
        """Test when the source table changes - should ignore column changes"""
        source = "SELECT id, name FROM customers_v1"
        target = "SELECT id, name FROM customers_v2"

        node = Node(
            unique_id="model.test.table_change", target_code=target, source_code=source
        )

        self.assertTrue(node.ignore_column_changes)
        self.assertEqual(node.column_changes, set())

    def test_join_condition_change(self):
        """Test when join conditions change - should ignore column changes"""
        source = """
            SELECT o.id, o.amount 
            FROM orders o 
            LEFT JOIN customers c ON o.customer_id = c.id
        """
        target = """
            SELECT o.id, o.amount 
            FROM orders o 
            INNER JOIN customers c ON o.customer_id = c.id
        """

        node = Node(
            unique_id="model.test.join_change", target_code=target, source_code=source
        )

        self.assertTrue(node.ignore_column_changes)
        self.assertEqual(node.column_changes, set())

    def test_where_clause_change(self):
        """Test when where clause changes - should ignore column changes"""
        source = "SELECT id, status FROM orders WHERE created_at > '2024-01-01'"
        target = "SELECT id, status FROM orders WHERE created_at > '2024-01-01' AND status != 'cancelled'"

        node = Node(
            unique_id="model.test.where_change", target_code=target, source_code=source
        )

        self.assertTrue(node.ignore_column_changes)
        self.assertEqual(node.column_changes, set())

    def test_multiple_column_changes(self):
        """Test multiple column changes - should track all changed columns"""
        source = "SELECT id, first_name as name, last_name as surname FROM users"
        target = "SELECT id, first_name as full_name, middle_name as middle FROM users"

        node = Node(
            unique_id="model.test.multiple_changes",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"name", "surname"})

    def test_window_function_change(self):
        """Test when window function changes - should be a breaking change"""
        source = """
            SELECT 
                id,
                ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY amount) as row_num
            FROM transactions
        """
        target = """
            SELECT 
                id,
                RANK() OVER (PARTITION BY category_id ORDER BY amount) as row_num
            FROM transactions
        """

        node = Node(
            unique_id="model.test.window_change", target_code=target, source_code=source
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"row_num"})

    def test_cte_changes_with_column_rename(self):
        """Test changes in CTEs - should not ignore column changes"""
        source = """
            WITH summary AS (
                SELECT category_id, SUM(amount) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total as category_total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """
        target = """
            WITH summary AS (
                SELECT category_id, SUM(amount * exchange_rate) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total as category_total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """

        node = Node(
            unique_id="model.test.cte_change_col_rename",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"category_total"})

    def test_cte_changes(self):
        """Test changes in CTEs - should not ignore column changes"""
        source = """
            WITH summary AS (
                SELECT category_id, SUM(amount) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """
        target = """
            WITH summary AS (
                SELECT category_id, SUM(amount * exchange_rate) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """

        node = Node(
            unique_id="model.test.cte_change", target_code=target, source_code=source
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"total"})

    def test_multiple_cte_changes(self):
        """Test changes in multiple CTEs - should not ignore column changes"""
        source = """
            WITH summary AS (
                SELECT category_id, SUM(amount) as total
                FROM transactions
                GROUP BY category_id
            ),
            
            cats as (
                SELECT id as category_id, name
                FROM categories
            )
            SELECT c.name, s.total
            FROM cats c
            JOIN summary s ON c.category_id = s.category_id
        """
        target = """
            WITH summary AS (
                SELECT category_id, SUM(amount * exchange_rate) as total
                FROM transactions
                GROUP BY category_id
            ),
            
            cats as (
                SELECT id as category_id, name || 's' as name
                FROM categories
            )
            SELECT c.name, s.total
            FROM cats c
            JOIN summary s ON c.category_id = s.category_id
        """

        node = Node(
            unique_id="model.test.multiple_cte_change",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"name", "total"})

    def test_multiple_cte_changes_with_column_rename(self):
        """Test changes in multiple CTEs - should not ignore column changes"""
        source = """
            WITH summary AS (
                SELECT category_id, SUM(amount) as total
                FROM transactions
                GROUP BY category_id
            ),
            
            cats as (
                SELECT id as category_id, name
                FROM categories
            )
            SELECT c.name, s.total
            FROM cats c
            JOIN summary s ON c.category_id = s.category_id
        """
        target = """
            WITH summary AS (
                SELECT category_id, SUM(amount * exchange_rate) as total
                FROM transactions
                GROUP BY category_id
            ),
            
            cats as (
                SELECT id as category_id, name || 's' as name
                FROM categories
            )
            SELECT c.name, s.total as new_total
            FROM cats c
            JOIN summary s ON c.category_id = s.category_id
        """

        node = Node(
            unique_id="model.test.multiple_cte_change_col_rename",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)
        self.assertEqual(node.column_changes, {"name", "total", "new_total"})

    def test_outside_cte_changes(self):
        """Test changes outside of CTEs - should not ignore column changes"""
        source = """
            WITH summary AS (
                SELECT category_id, SUM(amount) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """
        target = """
            WITH summary AS (
                SELECT category_id, SUM(amount * exchange_rate) as total
                FROM transactions
                GROUP BY category_id
            )
            SELECT c.name, s.total as category_total
            FROM categories c
            JOIN summary s ON c.id = s.category_id
        """

        node = Node(
            unique_id="model.test.outside_cte_change",
            target_code=target,
            source_code=source,
        )

        self.assertFalse(node.ignore_column_changes)

        # Ideally, this should only be total -> this is the column that was renamed
        # and the one we'll need to check downstream for.  category_total will not
        # exist in our CLL data, so it won't actually do anything except for make
        # an extra request to the Disco API.
        self.assertEqual(node.column_changes, {"category_total", "total"})