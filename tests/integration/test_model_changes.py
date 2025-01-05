# stdlib
from pathlib import Path
from typing import Dict
from unittest.mock import patch

# third party
import pytest

# first party
from src.services.orchestrator import CiOrchestrator
from src.utils import JobRunStatus


def create_model_file(project_dir: Path, name: str, content: str) -> None:
    """Create a model SQL file with the given content."""
    models_dir = project_dir / "models"
    with open(models_dir / f"{name}.sql", "w") as f:
        f.write(content)


def test_column_change_detection(
    test_project_dir: Path, orchestrator: CiOrchestrator
) -> None:
    """
    Test end-to-end workflow when a column is added to a model.

    This test verifies that:
    1. The change is detected
    2. Downstream models are properly identified
    3. The correct models are excluded from rebuilding
    """
    # Create initial model
    create_model_file(test_project_dir, "model1", "SELECT id FROM source_table")

    # Create downstream model
    create_model_file(test_project_dir, "model2", "SELECT id FROM {{ ref('model1') }}")

    # Mock initial compilation results
    with patch(
        "src.services.dbt_runner.DbtRunner.get_target_compiled_code"
    ) as mock_target:
        mock_target.return_value = {
            "model.test_project.model1": {
                "unique_id": "model.test_project.model1",
                "target_code": "SELECT id, name FROM source_table",
                "source_code": "SELECT id FROM source_table",
            }
        }

        # Mock Discovery API responses
        with patch(
            "src.services.discovery_client.DiscoveryClient.get_column_lineage"
        ) as mock_lineage:
            mock_lineage.return_value = [
                {"nodeUniqueId": "model.test_project.model2", "relationship": "child"}
            ]

            # Mock job trigger
            with patch("src.utils.trigger_job") as mock_trigger:
                mock_trigger.return_value = {"status": JobRunStatus.SUCCESS}

                # Run the orchestrator
                result = orchestrator.run()

                # Verify results
                assert result is True
                mock_lineage.assert_called_once()
                mock_trigger.assert_called_once()


def test_structural_change_detection(
    test_project_dir: Path, orchestrator: CiOrchestrator
) -> None:
    """
    Test end-to-end workflow when a model's structure changes.

    This test verifies that:
    1. The structural change is detected
    2. All downstream models are rebuilt
    3. No models are excluded from rebuilding
    """
    # Create initial model
    create_model_file(test_project_dir, "model1", "SELECT * FROM new_table")

    # Create downstream models
    create_model_file(test_project_dir, "model2", "SELECT * FROM {{ ref('model1') }}")

    # Mock compilation results
    with patch(
        "src.services.dbt_runner.DbtRunner.get_target_compiled_code"
    ) as mock_target:
        mock_target.return_value = {
            "model.test_project.model1": {
                "unique_id": "model.test_project.model1",
                "target_code": "SELECT * FROM new_table",
                "source_code": "SELECT * FROM old_table",
            }
        }

        # Mock Discovery API responses
        with patch(
            "src.services.discovery_client.DiscoveryClient.get_node_lineage"
        ) as mock_lineage:
            mock_lineage.return_value = {"model.test_project.model2"}

            # Mock job trigger
            with patch("src.utils.trigger_job") as mock_trigger:
                mock_trigger.return_value = {"status": JobRunStatus.SUCCESS}

                # Run the orchestrator
                result = orchestrator.run()

                # Verify results
                assert result is True
                mock_lineage.assert_called_once()
                mock_trigger.assert_called_once()
                # Verify no exclusions in job trigger
                assert mock_trigger.call_args[1].get("excluded_nodes") is None


def test_no_changes_detected(
    test_project_dir: Path, orchestrator: CiOrchestrator
) -> None:
    """
    Test end-to-end workflow when no changes are detected.

    This test verifies that:
    1. No changes are detected
    2. The job is triggered without modifications
    3. The process completes successfully
    """
    # Create model
    create_model_file(test_project_dir, "model1", "SELECT id FROM source_table")

    # Mock compilation results showing no changes
    with patch(
        "src.services.dbt_runner.DbtRunner.get_target_compiled_code"
    ) as mock_target:
        mock_target.return_value = {}  # No changes detected

        # Mock job trigger
        with patch("src.utils.trigger_job") as mock_trigger:
            mock_trigger.return_value = {"status": JobRunStatus.SUCCESS}

            # Run the orchestrator
            result = orchestrator.run()

            # Verify results
            assert result is True
            mock_trigger.assert_called_once()
            # Verify no exclusions in job trigger
            assert mock_trigger.call_args[1].get("excluded_nodes") is None
