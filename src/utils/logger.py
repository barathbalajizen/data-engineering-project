import logging
import json
import os
from datetime import datetime
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format."""

    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logger(name: str, log_level: str = None) -> logging.Logger:
    """
    Setup a logger with console and file handlers.

    Args:
        name: Logger name (typically __name__)
        log_level: Logging level (INFO, DEBUG, WARNING, ERROR). Defaults to env LOG_LEVEL

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Get log level from parameter or env, default to INFO
    level_str = log_level or os.getenv("LOG_LEVEL", "INFO")
    logger.setLevel(getattr(logging, level_str))

    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    # Console handler (simple format)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (JSON format)
    log_dir = Path(os.getenv("LOG_DIR", "./logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(
        log_dir / f"{name.replace('.', '_')}.log"
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = JsonFormatter()
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
