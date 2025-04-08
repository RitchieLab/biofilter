import pytest

# import sys
# from io import StringIO
from biofilter_modules.mixins.logger_mixin import LoggerMixin


class TestLoggerMixin(LoggerMixin):
    def __init__(self, verbose=True, quiet=False, log_file=None):
        self._verbose = verbose
        self._quiet = quiet
        self._logFile = log_file
        self._logIndent = 0
        self._logHanging = False


@pytest.fixture
def logger():
    return TestLoggerMixin()


def test_log_message(logger, capsys):
    logger.log("Test message")
    captured = capsys.readouterr()
    assert captured.err == "Test message"


def test_warn_message(logger, capsys):
    logger.warn("Warning message")
    captured = capsys.readouterr()
    assert captured.err == "Warning message"


def test_log_push(logger, capsys):
    logger.logPush("Pushed message")
    captured = capsys.readouterr()
    assert captured.err == "Pushed message\n"


def test_log_pop(logger, capsys):
    logger.logPush("Pushed message")
    logger.logPop("Popped message")
    captured = capsys.readouterr()
    assert captured.err == "Pushed message\nPopped message"


def test_warn_push(logger, capsys):
    logger.warnPush("Pushed warning")
    captured = capsys.readouterr()
    assert captured.err == "Pushed warning\n"


def test_warn_pop(logger, capsys):
    logger.warnPush("Pushed warning")
    logger.warnPop("Popped warning")
    captured = capsys.readouterr()
    assert captured.err == "Pushed warning\nPopped warning"


def test_log_with_indentation(logger, capsys):
    logger.logPush("Level 1")
    logger.log("Level 1 message")
    logger.logPush("Level 2")
    logger.log("Level 2 message")
    logger.logPop("Back to Level 1")
    logger.logPop("Back to Level 0")
    captured = capsys.readouterr()
    assert (
        captured.err
        == "Level 1\n  Level 1 messageLevel 2\n    Level 2 message\n  Back to Level 1\nBack to Level 0"  # noqa: E501
    )


def test_warn_with_indentation(logger, capsys):
    logger.warnPush("Level 1")
    logger.warn("Level 1 warning")
    logger.warnPush("Level 2")
    logger.warn("Level 2 warning")
    logger.warnPop("Back to Level 1")
    logger.warnPop("Back to Level 0")
    captured = capsys.readouterr()
    assert (
        captured.err
        == "Level 1\n  Level 1 warningLevel 2\n    Level 2 warning\n  Back to Level 1\nBack to Level 0"  # noqa: E501
    )


def test_log_indentation(logger, capsys):
    logger._logIndent = 2
    logger._log("Indented message")
    captured = capsys.readouterr()
    assert captured.err == "    Indented message"
    assert logger._logHanging is True


def test_log_no_newline(logger, capsys):
    logger._log("Message without newline")
    captured = capsys.readouterr()
    assert captured.err == "Message without newline"
    assert logger._logHanging is True


def test_log_with_newline(logger, capsys):
    logger._log("Message with newline\n")
    captured = capsys.readouterr()
    assert captured.err == "Message with newline\n"
    assert logger._logHanging is False


def test_warn_message_with_newline(logger, capsys):
    logger._log("Warning message with newline\n", warning=True)
    captured = capsys.readouterr()
    assert captured.err == "Warning message with newline\n"
    assert logger._logHanging is False


def test_warn_message_without_newline(logger, capsys):
    logger._log("Warning message without newline", warning=True)
    captured = capsys.readouterr()
    assert captured.err == "Warning message without newline"
    assert logger._logHanging is True


def test_log_to_file(tmp_path):
    log_file = tmp_path / "log.txt"
    logger = TestLoggerMixin(log_file=log_file.open("w"))
    logger._log("Message to file\n")
    logger._logFile.close()
    with log_file.open("r") as f:
        content = f.read()
    assert content == "Message to file\n"


def test_warn_to_file(tmp_path):
    log_file = tmp_path / "log.txt"
    logger = TestLoggerMixin(log_file=log_file.open("w"))
    logger._log("Warning to file\n", warning=True)
    logger._logFile.close()
    with log_file.open("r") as f:
        content = f.read()
    assert content == "Warning to file\n"


def test_log_indentation_to_file(tmp_path, logger, capsys):
    log_file = tmp_path / "log.txt"
    logger = TestLoggerMixin(log_file=log_file.open("w"))
    logger._logIndent = 2
    logger._log("Indented message")
    logger._logFile.close()
    with log_file.open("r") as f:
        content = f.read()
    assert content == "    Indented message"
