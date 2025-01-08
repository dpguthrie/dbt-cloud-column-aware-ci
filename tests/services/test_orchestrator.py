import logging
from unittest.mock import patch

import pytest

from src.services.orchestrator import CiOrchestrator
from src.utils import JobRunStatus


@pytest.fixture
def orchestrator(mock_config, mock_dbt_runner):
    """Create a CiOrchestrator instance with mocked dependencies."""
    return CiOrchestrator(config=mock_config, dbt_runner=mock_dbt_runner)


def test_post_init_creates_dbt_runner(mock_config):
    """Test that dbt_runner is created if not provided."""
    with patch("src.services.orchestrator.DbtRunner") as mock_dbt_runner_cls:
        orchestrator = CiOrchestrator(config=mock_config)
        mock_dbt_runner_cls.assert_called_once_with(mock_config)
        assert orchestrator.dbt_runner == mock_dbt_runner_cls.return_value


def test_setup(orchestrator):
    """Test setup creates dbt cloud profile."""
    with patch(
        "src.services.orchestrator.create_dbt_cloud_profile"
    ) as mock_create_profile:
        orchestrator.setup()
        mock_create_profile.assert_called_once_with(orchestrator.config)


def test_compile_and_get_nodes_with_changes(orchestrator, mock_dbt_runner):
    """Test compile_and_get_nodes when there are modified models."""
    target_nodes = {"model.test": {"target": "code1"}}
    source_nodes = {"model.test": {"source": "code2"}}
    expected = {"model.test": {"target": "code1", "source": "code2"}}

    mock_dbt_runner.get_target_compiled_code.return_value = target_nodes
    mock_dbt_runner.get_source_compiled_code.return_value = source_nodes

    result = orchestrator.compile_and_get_nodes()

    mock_dbt_runner.compile_models.assert_called_once()
    mock_dbt_runner.get_target_compiled_code.assert_called_once()
    mock_dbt_runner.get_source_compiled_code.assert_called_once_with(["model.test"])
    assert result == expected


def test_target_nodes_change_sources_nodes_no_change(
    orchestrator, mock_dbt_runner, caplog
):
    """Test compile_and_get_nodes when there are modified models."""
    caplog.set_level(logging.INFO)
    target_nodes = {"model.test": {"target": "code1"}}
    source_nodes = {}
    expected = {}

    mock_dbt_runner.get_target_compiled_code.return_value = target_nodes
    mock_dbt_runner.get_source_compiled_code.return_value = source_nodes

    result = orchestrator.compile_and_get_nodes()

    mock_dbt_runner.compile_models.assert_called_once()
    mock_dbt_runner.get_target_compiled_code.assert_called_once()
    mock_dbt_runner.get_source_compiled_code.assert_called_once_with(["model.test"])

    assert "Modified resources `model.test`" in caplog.text
    assert result == expected


def test_compile_and_get_nodes_no_changes(orchestrator, mock_dbt_runner):
    """Test compile_and_get_nodes when there are no modified models."""
    mock_dbt_runner.get_target_compiled_code.return_value = {}

    result = orchestrator.compile_and_get_nodes()

    assert result == {}
    mock_dbt_runner.compile_models.assert_called_once()
    mock_dbt_runner.get_source_compiled_code.assert_not_called()


def test_get_excluded_nodes(orchestrator, mock_dbt_runner, sample_compiled_nodes):
    """Test get_excluded_nodes returns correct list."""
    mock_dbt_runner.get_all_unique_ids.return_value = {"model.my_project.downstream"}

    result = orchestrator.get_excluded_nodes(sample_compiled_nodes)

    mock_dbt_runner.get_all_unique_ids.assert_called_once_with(
        ["model.my_project.first_model", "model.my_project.second_model"]
    )
    assert isinstance(result, list)


def test_trigger_and_check_job_success(orchestrator):
    """Test trigger_and_check_job with successful run."""
    with patch("src.services.orchestrator.trigger_job") as mock_trigger:
        mock_trigger.return_value = {"status": JobRunStatus.SUCCESS}

        result = orchestrator.trigger_and_check_job(excluded_nodes=["model.test"])

        assert result is True
        mock_trigger.assert_called_once_with(
            orchestrator.config, excluded_nodes=["model.test"]
        )


def test_trigger_and_check_job_failure(orchestrator):
    """Test trigger_and_check_job with failed run."""
    with patch("src.services.orchestrator.trigger_job") as mock_trigger:
        mock_trigger.return_value = {"status": JobRunStatus.ERROR}

        result = orchestrator.trigger_and_check_job()

        assert result is False
        mock_trigger.assert_called_once_with(orchestrator.config, excluded_nodes=None)


def test_run_success(orchestrator):
    """Test successful run of the entire orchestration process."""
    with (
        patch.object(orchestrator, "setup") as mock_setup,
        patch.object(orchestrator, "compile_and_get_nodes") as mock_compile,
        patch.object(orchestrator, "get_excluded_nodes") as mock_get_excluded,
        patch.object(orchestrator, "trigger_and_check_job") as mock_trigger,
    ):
        mock_compile.return_value = {"model.test": {"some": "data"}}
        mock_get_excluded.return_value = ["excluded.model"]
        mock_trigger.return_value = True

        result = orchestrator.run()

        assert result is True
        mock_setup.assert_called_once()
        mock_compile.assert_called_once()
        mock_get_excluded.assert_called_once()
        mock_trigger.assert_called_once_with(["excluded.model"])


def test_run_with_no_changes(orchestrator):
    """Test run when no models are modified."""
    with (
        patch.object(orchestrator, "setup") as mock_setup,
        patch.object(orchestrator, "compile_and_get_nodes") as mock_compile,
        patch.object(orchestrator, "trigger_and_check_job") as mock_trigger,
    ):
        mock_compile.return_value = {}
        mock_trigger.return_value = True

        result = orchestrator.run()

        assert result is True
        mock_setup.assert_called_once()
        mock_compile.assert_called_once()
        mock_trigger.assert_called_once()


def test_run_handles_exceptions(orchestrator, caplog):
    """Test run handles exceptions gracefully."""
    with patch.object(orchestrator, "setup", side_effect=Exception("Test error")):
        result = orchestrator.run()

        assert result is False
        assert "Error during CI process: Test error" in caplog.text


def test_trigger_and_check_job_dry_run_no_nodes(orchestrator, mock_config, caplog):
    """Test dry run with no excluded nodes."""
    caplog.set_level(logging.INFO)
    mock_config.dry_run = True

    result = orchestrator.trigger_and_check_job()

    assert result is True
    assert "DRY RUN MODE" in caplog.text
    assert (
        "Models that would've been excluded from the build are listed below:"
        in caplog.text
    )
    # Verify empty list produces no node entries
    assert " - " not in caplog.text


def test_trigger_and_check_job_dry_run_with_nodes(orchestrator, mock_config, caplog):
    """Test dry run with excluded nodes."""
    caplog.set_level(logging.INFO)
    mock_config.dry_run = True
    excluded_nodes = ["node1", "node2"]

    result = orchestrator.trigger_and_check_job(excluded_nodes=excluded_nodes)

    assert result is True
    assert "DRY RUN MODE" in caplog.text
    assert "2" in caplog.text
    assert "node1" in caplog.text
    assert "node2" in caplog.text


def test_trigger_and_check_job_not_dry_run(orchestrator, mock_config):
    """Test normal run (not dry run) still works."""
    mock_config.dry_run = False

    with patch("src.services.orchestrator.trigger_job") as mock_trigger_job:
        mock_trigger_job.return_value = {"status": "success"}
        excluded_nodes = ["model.test.node1"]

        result = orchestrator.trigger_and_check_job(excluded_nodes=excluded_nodes)

        assert result is True
        mock_trigger_job.assert_called_once_with(
            mock_config, excluded_nodes=excluded_nodes
        )
