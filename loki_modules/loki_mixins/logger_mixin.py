# #################################################
# LOGING MIXIN
# #################################################
import os
import traceback
import logging
from logging.handlers import RotatingFileHandler
from colorama import Fore, Style


class LoggerMixin:
    def init_logger(self, log_file="loki.log", log_level=logging.INFO):
        """
        Initializes the logger.

        Args:
            log_file (str): Path to the log file.
            log_level (int): Logging level (e.g., logging.INFO).
        """
        self._log_file = os.path.abspath(log_file)
        self._logger = logging.getLogger(self.__class__.__name__)
        if not self._logger.handlers:  # Avoid adding multiple handlers
            # Set up file handler
            file_handler = RotatingFileHandler(
                log_file, maxBytes=5 * 1024 * 1024, backupCount=3
            )
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            self._logger.addHandler(file_handler)

            # Set up console handler with colors
            if self.getVerbose():
                console_handler = logging.StreamHandler()
                console_formatter = logging.Formatter(
                    "%(asctime)s - %(name)s: %(message)s"
                )
                console_handler.setFormatter(
                    self.ColoredFormatter(console_formatter)
                )  # noqa: E501
                self._logger.addHandler(console_handler)

            self._logger.setLevel(log_level)

    def log(self, message="", level=logging.INFO, indent=0):
        """
        Logs a message with the specified level and indent.

        Args:
            message (str): The message to log.
            level (int): Logging level (e.g., logging.INFO).
            indent (int): Number of spaces to indent the log level.
        """
        if message:
            indented_message = " " * indent + message
            self._logger.log(level, indented_message)

    class ColoredFormatter(logging.Formatter):
        """
        Custom formatter to add colors to log messages based on their level.
        """

        COLORS = {
            logging.DEBUG: Fore.BLUE,
            logging.INFO: Fore.LIGHTBLACK_EX,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.GREEN,
        }

        def __init__(self, formatter):
            super().__init__()
            self.formatter = formatter

        def format(self, record):
            color = self.COLORS.get(record.levelno, Fore.WHITE)
            message = self.formatter.format(record)
            return f"{color}{message}{Style.RESET_ALL}"

    def log_exception(self, error):
        """
        Logs an exception with traceback.

        Args:
            error (Exception): The exception object to log.
        """
        if self._logger:
            self._logger.error("Exception occurred: %s", str(error))
            self._logger.error(traceback.format_exc())

        """
        How to usem this method:
        << BEFORE >>
        try:
            # Code with error
        except Exception as e:
            print("Error:", e)

        << NOW >>
        try:
            # Code with error
        except Exception as e:
            self.log_exception(e)
            raise ...
        """

    def get_log_file(self):
        """
        Returns the absolute path to the log file.

        Returns:
            str: Absolute path to the log file.
        """
        return self._log_file

    def getVerbose(self):
        """
        Gets the verbosity setting.

        Returns:
            bool: True if verbose logging is enabled, otherwise False.
        """
        return self._verbose

    def setVerbose(self, verbose=True):
        """
        Sets the verbosity setting.

        Args:
            verbose (bool, optional): True to enable verbose logging, False to
            disable.
        """
        self._verbose = verbose
