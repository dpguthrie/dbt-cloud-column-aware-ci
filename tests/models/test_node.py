# third party
import pytest

# first party
from src.models.node import Node, NodeFactory, NodeManager


def test_node_initialization():
    """Test basic node initialization with simple SQL."""
    node = Node(
        unique_id="model.my_project.test",
        source_code="SELECT id FROM table",
        target_code="SELECT id, name FROM table",
        dialect="snowflake",
    )

    assert node.unique_id == "model.my_project.test"
    assert len(node.changes) > 0
    assert len(node.breaking_changes) == 0  # Adding columns isn't breaking
    assert not node.ignore_column_changes
    assert len(node.column_changes) == 0


def test_node_with_breaking_changes():
    """Test node with breaking changes (column removal)."""
    node = Node(
        unique_id="model.my_project.test",
        source_code="SELECT id, name, age FROM table",
        target_code="SELECT id, name FROM table",
        dialect="snowflake",
    )

    assert len(node.breaking_changes) > 0
    assert not node.ignore_column_changes
    assert "age" in node.column_changes


def test_node_with_invalid_sql():
    """Test node initialization with invalid SQL."""
    node = Node(
        unique_id="model.my_project.test",
        source_code="INVALID SQL",
        target_code="MORE INVALID SQL",
        dialect="snowflake",
    )

    assert len(node.changes) == 0
    assert len(node.breaking_changes) == 0


def test_node_factory(sample_compiled_nodes):
    """Test NodeFactory creates nodes correctly."""
    nodes = NodeFactory.create_nodes(sample_compiled_nodes, "snowflake")

    assert len(nodes) == 2
    assert "model.my_project.first_model" in nodes
    assert "model.my_project.second_model" in nodes
    assert all(isinstance(node, Node) for node in nodes.values())


class TestNodeManager:
    @pytest.fixture
    def node_manager(self, mock_config, sample_compiled_nodes, mock_lineage_service):
        all_unique_ids = {
            "model.my_project.first_model",
            "model.my_project.second_model",
        }
        return NodeManager(
            config=mock_config,
            all_nodes=sample_compiled_nodes,
            all_unique_ids=all_unique_ids,
            lineage_service=mock_lineage_service,
        )

    def test_node_manager_initialization(self, node_manager):
        """Test NodeManager initializes correctly."""
        assert len(node_manager.nodes) == 2
        assert len(node_manager.node_unique_ids) == 2
        assert isinstance(node_manager.nodes[0], Node)

    def test_get_excluded_nodes_empty(self, mock_config, mock_lineage_service):
        """Test get_excluded_nodes with empty nodes."""
        manager = NodeManager(
            config=mock_config,
            all_nodes={},
            all_unique_ids=set(),
            lineage_service=mock_lineage_service,
        )
        assert manager.get_excluded_nodes() == []

    def test_get_excluded_nodes_with_changes(self, node_manager, mock_lineage_service):
        """Test get_excluded_nodes with actual changes."""
        # Setup mock lineage service to return some impacted nodes
        mock_lineage_service.get_node_lineage.return_value = {
            "model.my_project.first_model"
        }

        excluded = node_manager.get_excluded_nodes()
        assert isinstance(excluded, list)
        # Should exclude second_model as it's not in the impacted set
        assert "second_model" in excluded


def test_node_with_structural_changes():
    """Test node with structural (non-column) changes."""
    node = Node(
        unique_id="model.my_project.test",
        source_code="SELECT id FROM table1",
        target_code="SELECT id FROM table2",  # Changed source table
        dialect="snowflake",
    )

    assert len(node.breaking_changes) > 0
    assert (
        node.ignore_column_changes
    )  # Should ignore column changes due to structural change
    assert len(node.column_changes) == 0


def test_node_with_udtf_changes():
    """Test node with UDTF (User Defined Table Function) changes."""
    node = Node(
        unique_id="model.my_project.test",
        source_code="SELECT * FROM TABLE(my_udtf(col1))",
        target_code="SELECT * FROM TABLE(my_udtf(col1, col2))",
        dialect="snowflake",
    )

    assert len(node.breaking_changes) > 0
    assert node.ignore_column_changes
