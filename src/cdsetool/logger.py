"""
Logging utilities.

This module provides a NoopLogger class that outputs nothing.
"""

import logging
import sys
from typing import Any


class NoopLogger:
    """
    A logger that does nothing.
    """

    def debug(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a debug message.
        """

    def error(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an error message.
        """

    def info(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an info message.
        """

    def warning(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a warning message.
        """


class ConsoleLogger:
    """
    A logger that outputs to stderr using Python's logging module.
    """

    def __init__(self, level: int = logging.WARNING) -> None:
        self._logger = logging.getLogger("cdsetool")
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
            self._logger.addHandler(handler)
        self._logger.setLevel(level)

    def debug(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a debug message.
        """
        self._logger.debug(msg, *args, **kwargs)

    def error(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an error message.
        """
        self._logger.error(msg, *args, **kwargs)

    def info(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an info message.
        """
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a warning message.
        """
        self._logger.warning(msg, *args, **kwargs)
