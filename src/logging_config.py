import json
import logging
import logging.config
from typing import Any, Dict


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Get the standard formatting
        message = super().format(record)

        # Handle the extra dict if it exists
        if hasattr(record, "extra"):
            try:
                extra_formatted = json.dumps(record.extra, default=str)
                return f"{message} - extra: {extra_formatted}"
            except Exception:
                return f"{message} - extra: {str(record.extra)}"
        return message


DEFAULT_LOGGING_CONFIG: Dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "()": StructuredFormatter,  # Use our custom formatter
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "src": {  # Root logger for your application
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the application.

    Args:
        level: The logging level to use (e.g., "DEBUG", "INFO", "WARNING"). Defaults to "INFO".
    """
    config = DEFAULT_LOGGING_CONFIG.copy()
    config["loggers"]["src"]["level"] = level
    logging.config.dictConfig(config)
