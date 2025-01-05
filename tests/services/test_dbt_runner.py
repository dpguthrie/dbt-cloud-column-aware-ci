# stdlib
import json
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, mock_open, patch

# third party
import pytest

# first party
from src.services.dbt_runner import DbtRunner


@pytest.fixture
def dbt_runner(mock_config, mock_discovery_client) -> DbtRunner:
    """Create a DbtRunner instance with mocked dependencies."""
    return DbtRunner(config=mock_config, _discovery_client=mock_discovery_client)


def test_compile_models_success(dbt_runner: DbtRunner) -> None:
    """Test successful model compilation."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0

        # Should not raise an exception
        dbt_runner.compile_models()

        # Verify correct command was called
        mock_run.assert_called_once_with(
            DbtRunner.DBT_COMMANDS["compile"], capture_output=True
        )


def test_compile_models_failure(dbt_runner: DbtRunner) -> None:
    """Test failed model compilation."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = b"Compilation error"

        with pytest.raises(RuntimeError, match="Failed to compile models"):
            dbt_runner.compile_models()


def test_get_target_compiled_code(dbt_runner: DbtRunner) -> None:
    """Test retrieval of compiled code from run_results.json."""
    mock_run_results = {
        "results": [
            {
                "unique_id": "model.project.test_model",
                "compiled_code": "SELECT * FROM table",
                "relation_name": "test_model",
            }
        ]
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_run_results))):
        result = dbt_runner.get_target_compiled_code()

        assert len(result) == 1
        assert "model.project.test_model" in result
        assert (
            result["model.project.test_model"]["target_code"] == "SELECT * FROM table"
        )


def test_get_target_compiled_code_no_file(dbt_runner: DbtRunner) -> None:
    """Test handling of missing run_results.json."""
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = FileNotFoundError()

        result = dbt_runner.get_target_compiled_code()
        assert result == {}


def test_get_source_compiled_code(dbt_runner: DbtRunner) -> None:
    """Test retrieval of compiled code from Discovery API."""
    mock_compiled_code = {
        "model.project.test_model": {"source_code": "SELECT * FROM table"}
    }
    dbt_runner._discovery_client.get_compiled_code.return_value = mock_compiled_code

    result = dbt_runner.get_source_compiled_code(["model.project.test_model"])

    assert result == mock_compiled_code
    dbt_runner._discovery_client.get_compiled_code.assert_called_once_with(
        dbt_runner.config.dbt_cloud_environment_id, ["model.project.test_model"]
    )


def test_get_all_unique_ids(dbt_runner: DbtRunner) -> None:
    """Test retrieval of all affected unique IDs."""
    mock_stdout = """
    {"uniqueId": "model.project.downstream1"}
    {"uniqueId": "model.project.downstream2"}
    """

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = mock_stdout

        result = dbt_runner.get_all_unique_ids(["model.project.source"])

        assert len(result) == 2
        assert "model.project.downstream1" in result
        assert "model.project.downstream2" in result
        mock_run.assert_called_once_with(
            DbtRunner.DBT_COMMANDS["ls"], capture_output=True, text=True
        )


def test_get_all_unique_ids_command_failure(dbt_runner: DbtRunner) -> None:
    """Test handling of dbt ls command failure."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "Command failed"

        result = dbt_runner.get_all_unique_ids(["model.project.source"])

        assert result == set()
