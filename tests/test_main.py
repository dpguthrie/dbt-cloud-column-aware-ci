import logging
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_setup():
    """Fixture to set up common mocks."""
    with (
        patch("src.main.Config.from_env") as mock_config,
        patch("src.main.CiOrchestrator") as mock_orchestrator,
        patch("src.main.setup_logging") as mock_logging,
    ):
        # Configure the mocks
        mock_orchestrator_instance = mock_orchestrator.return_value
        mock_orchestrator_instance.run.return_value = True

        yield {
            "config": mock_config,
            "orchestrator": mock_orchestrator,
            "orchestrator_instance": mock_orchestrator_instance,
            "logging": mock_logging,
        }


def test_main_successful_execution(mock_setup):
    """Test main function with successful execution."""
    # Set return value for the mock instance from the fixture
    mock_setup["orchestrator_instance"].run.return_value = True

    with pytest.raises(SystemExit) as exit_info:
        import src.main

        src.main.main()

    assert exit_info.value.code == 0

    # Verify our mocks were called correctly
    mock_setup["logging"].assert_called_once()
    mock_setup["config"].assert_called_once()
    mock_setup["orchestrator"].assert_called_once_with(
        mock_setup["config"].return_value
    )
    mock_setup["orchestrator_instance"].run.assert_called_once()


def test_main_failed_execution(mock_setup, caplog):
    """Test main function when orchestrator run fails."""

    mock_setup["orchestrator_instance"].run.return_value = False

    with pytest.raises(SystemExit) as exit_info:
        import src.main

        src.main.main()

    assert exit_info.value.code == 1


def test_main_exception_handling(mock_setup, caplog):
    """Test main function when an exception occurs."""
    caplog.set_level(logging.ERROR)

    mock_setup["orchestrator_instance"].run.side_effect = Exception("Test error")

    with pytest.raises(SystemExit):
        import src.main

        src.main.main()

    # Verify error was logged
    assert "Fatal error in main process" in caplog.text
