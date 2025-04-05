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

    def _initialize(self, log_file, log_level):
        """Initializes the logger configuration."""
        init(autoreset=True)  # Enables color formatting in terminal

        self.logger = logging.getLogger("BiofilterLogger")
        self.logger.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))

        # ✅ Prevent duplicate handlers
        if not self.logger.hasHandlers():
            # Creating file handler
            log_path = os.path.join(os.getcwd(), log_file)
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )

            # Creating console handler with color formatting
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(self.ColoredFormatter())

            # Adding handlers only if not already added
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

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
            return f"{log_color}[{record.levelname}] {record.msg}{Style.RESET_ALL}"


# 🛠️ HOW TO USE IT:
# from omics_modules.logger import OmicsLogger

# # Create a new instance of the logger
# logger = OmicsLogger()

# # Logs with different levels
# logger.log("This is a DEBUG", logging.DEBUG)
# logger.log("This is a INFO", logging.INFO)
# logger.log("This is a WARNING", logging.WARNING)
# logger.log("This is an ERROR", logging.ERROR)
# logger.log("This is a CRITICAL", logging.CRITICAL)
