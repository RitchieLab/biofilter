# import os
import re
import logging

# import tempfile
from biofilter.utils.logger import Logger


def test_singleton_behavior():
    logger1 = Logger(log_file="test_logger.log")
    logger2 = Logger(log_file="another_file.log")

    assert logger1 is logger2
    assert logger1.logger.name == "BiofilterLogger"


def test_logging_to_file(tmp_path):
    # Completely reset to ensure a clean environment
    logging.getLogger("BiofilterLogger").handlers.clear()
    Logger._instance = None
    log_path = tmp_path / "logfile.log"

    logger = Logger(log_file=str(log_path))
    logger.log("This is a test message", "INFO")

    assert log_path.exists(), "Log file was not created"
    content = log_path.read_text()
    assert "This is a test message" in content
    assert "INFO" in content


def test_log_level_changes(tmp_path):
    log_path = tmp_path / "logfile.log"
    logger = Logger(log_file=log_path.name)

    logger.set_log_level("DEBUG")
    assert logger.logger.level == logging.DEBUG

    logger.set_log_level("ERROR")
    assert logger.logger.level == logging.ERROR


def test_log_levels(tmp_path):
    # Complete reset to ensure a clean environment
    logging.getLogger("BiofilterLogger").handlers.clear()
    Logger._instance = None

    log_path = tmp_path / "logfile.log"

    logger = Logger(log_file=str(log_path))
    logger.set_log_level("DEBUG")

    logger.log("debug test", "DEBUG")
    logger.log("info test", "INFO")
    logger.log("warn test", "WARNING")
    logger.log("error test", "ERROR")
    logger.log("critical test", "CRITICAL")

    content = log_path.read_text()

    assert "debug test" in content
    assert "info test" in content
    assert "warn test" in content
    assert "error test" in content
    assert "critical test" in content


def test_colored_formatter_formatting():
    formatter = Logger.ColoredFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="",
        lineno=0,
        msg="Test colored output",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    assert "[WARNING]" in formatted
    assert "Test colored output" in formatted


def test_log_levels_with_counts_and_format(tmp_path):
    # Reset singleton and clear handlers
    logging.getLogger("BiofilterLogger").handlers.clear()
    Logger._instance = None

    log_path = tmp_path / "expanded_log.log"

    logger = Logger(log_file=str(log_path))
    logger.set_log_level("DEBUG")

    logger.log("debug test", "DEBUG")
    logger.log("info test", "INFO")
    logger.log("warn test", "WARNING")
    logger.log("error test", "ERROR")
    logger.log("critical test", "CRITICAL")

    content = log_path.read_text()

    # Validate presence of messages
    for msg in [
        "debug test",
        "info test",
        "warn test",
        "error test",
        "critical test",
    ]:  # noqa E501
        assert msg in content

    # Count log level tags using regex
    levels = ["INFO", "WARNING", "ERROR", "CRITICAL"]
    for level in levels:
        matches = re.findall(rf"\b{level}\b", content)
        assert (
            len(matches) == 1
        ), f"Expected 1 log entry for level {level}, found {len(matches)}"  # noqa E501

    # Validate timestamp format
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}"
    timestamps = re.findall(timestamp_pattern, content)
    assert len(timestamps) == 6, "Expected 5 timestamps, one per log entry"

    # Full line format check
    for line in content.strip().splitlines():
        assert re.match(
            rf"^{timestamp_pattern} - (DEBUG|INFO|WARNING|ERROR|CRITICAL) - .+$",  # noqa E501
            line,
        )
