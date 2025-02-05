import logging
import os
from colorama import init, Fore, Style


class OmicsLogger:
    """
    Singleton class to manage logs in a centralized way with colors in the
    terminal.
    """

    _instance = None  # Armazena a instância única

    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __new__(cls, log_file="omics.log", log_level=logging.INFO):
        if cls._instance is None:
            cls._instance = super(OmicsLogger, cls).__new__(cls)
            cls._instance._initialize(log_file, log_level)
        return cls._instance

    def _initialize(self, log_file, log_level):
        """Inicializa a configuração do logger."""
        init(autoreset=True)  # Ativa cores no terminal

        self.logger = logging.getLogger("OmicsLogger")
        self.logger.setLevel(log_level)

        # Criando handler para arquivo de log
        log_path = os.path.join(os.getcwd(), log_file)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )  # noqa E501

        # Criando handler para console com cores
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.ColoredFormatter())

        # Adiciona os handlers ao logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def log(self, message, level="INFO"):
        """
        Logs a message with the specified level.

        Args:
            message (str): The message to be logged.
            level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        """
        level = self.LOG_LEVELS.get(level.upper(), logging.INFO)  # Convert string to logging level
        if self.logger:
            self.logger.log(level, message)

    def set_log_level(self, log_level):
        """Permite mudar dinamicamente o nível de log."""
        # self.logger.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))
        level = self.LOG_LEVELS.get(log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        print(f"[DEBUG] Logger level set to {log_level.upper()}")  # Debugging the logger level

    class ColoredFormatter(logging.Formatter):
        """Formatter para adicionar cores ao console."""
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
