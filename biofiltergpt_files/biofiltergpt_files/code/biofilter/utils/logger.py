import logging
import os
from colorama import init, Fore, Style


class Logger:
    """
    Singleton class to manage logs in a centralized way with colors in the
    terminal.
    """

    _instance = None  # Stores the singleton instance

    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __new__(cls, log_file="biofilter.log", log_level="INFO"):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize(log_file, log_level)
        return cls._instance

    # NOTE: DELETE this block of code when the logger is working
    # def _initialize(self, log_file, log_level):
    #     """Initializes the logger configuration."""
    #     init(autoreset=True)  # Enables color formatting in terminal

    #     self.logger = logging.getLogger("BiofilterLogger")
    #     self.logger.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))  # noqa E501

    #     # ✅ Prevent duplicate handlers
    #     if not self.logger.hasHandlers():
    #         # Creating file handler
    #         # log_path = os.path.join(os.getcwd(), log_file)
    #         log_path = os.path.abspath(log_file)
    #         file_handler = logging.FileHandler(log_path)
    #         file_handler.setFormatter(
    #             logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")  # noqa E501
    #         )

    #         # Creating console handler with color formatting
    #         console_handler = logging.StreamHandler()
    #         console_handler.setFormatter(self.ColoredFormatter())

    #         # Adding handlers only if not already added
    #         self.logger.addHandler(file_handler)
    #         self.logger.addHandler(console_handler)
    def _initialize(self, log_file, log_level):
        if getattr(self, "_configured", False):
            return  # Already configured, exit early

        init(autoreset=True)

        self.logger = logging.getLogger("BiofilterLogger")
        self.logger.setLevel(
            self.LOG_LEVELS.get(log_level.upper(), logging.INFO)
        )  # noqa E501

        # File handler
        log_path = os.path.abspath(log_file)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.ColoredFormatter())

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self._configured = True  # Check if the logger is already configured

    def log(self, message, level="INFO"):
        """
        Logs a message with the specified level.

        Args:
            message (str): The message to be logged.
            level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        """
        log_level = self.LOG_LEVELS.get(level.upper(), logging.INFO)
        self.logger.log(log_level, message)

    def set_log_level(self, log_level):
        """Allows changing the log level dynamically."""
        level = self.LOG_LEVELS.get(log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        self.log(f"Logger level set to {log_level.upper()}", "DEBUG")

    class ColoredFormatter(logging.Formatter):
        """Formatter that adds colors to console output."""

        COLORS = {
            logging.DEBUG: Fore.CYAN,
            logging.INFO: Fore.GREEN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.RED + Style.BRIGHT,
        }

        def format(self, record):
            log_color = self.COLORS.get(record.levelno, Fore.WHITE)
            return f"{log_color}[{record.levelname}] {record.msg}{Style.RESET_ALL}"  # noqa E501


"""
================================================================================
Developer Note - Logger Utility
================================================================================

This module provides a centralized, singleton-based logger for the Biofilter
system, designed to be reused across all components and services.

Key Features:

- Singleton pattern:
    Ensures consistent logging behavior and configuration across the entire
    system.
    Only one instance of the logger will ever exist, regardless of how many
    times it is imported or instantiated.

- Colored terminal output:
    Uses `colorama` to differentiate log levels visually, improving CLI
    readability.

- File and console logging:
    Simultaneously logs to both console and file, with independent formatters
    (color for console, plain for file).

- Dynamic reconfiguration:
    Supports setting log level dynamically at runtime via `set_log_level()`.

- Test-friendly architecture:
    The singleton can be reset for testing by clearing the `_instance`
    attribute and manually clearing handlers from
    `logging.getLogger("BiofilterLogger")`.

Design Notes:

- To avoid unintended reconfiguration or duplicate handlers, the class uses an
    internal `_configured` flag in combination with direct handler inspection
    to guard the initialization block.

- Log messages use a consistent format with timestamp, level, and message
    content, Example:
        `2025-04-05 22:15:34,112 - INFO - Data ingestion completed`

- Developers should avoid reusing `logging.getLogger(...)` directly in modules.
    Always use `Logger().log(...)` for consistency.

# HOW TO USE IT:

from biofilter.utils.logger import Logger

logger = Logger()
logger.log("System initialized", "INFO")
logger.set_log_level("DEBUG")
logger.log("This is a DEBUG", logging.DEBUG)
logger.log("This is a INFO", logging.INFO)
logger.log("This is a WARNING", logging.WARNING)
logger.log("This is an ERROR", logging.ERROR)
logger.log("This is a CRITICAL", logging.CRITICAL)
"""
