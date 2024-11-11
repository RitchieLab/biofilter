# #################################################
# LOGING MIXIN
# #################################################
import sys


class LoggerMixin:
    """
    Logger is a centralized logging utility that provides structured logging
    with indentation management. This class allows consistent logging across
    different components and can differentiate between standard log messages
    and warnings. It supports logging to both standard output and an optional
    log file, with control over indentation levels for better readability.

    IMPLEMENTED METHODS:
    - [log]: Logs a standard message to both file and standard output if apply.
    - [warn]: Logs a warning message.
    - [logPush]: Increases the indentation level and optionally logs a message.
    - [logPop]: Decreases the indentation level and optionally logs a message.
    - [warnPush]: Increases indentation for warnings.
    - [warnPop]: Decreases indentation for warnings.
    """

    def _log(self, message="", warning=False):
        """
        Logs a message to the log file and/or standard error output.

        Parameters:
        message (str): The message to log. Defaults to an empty string.
        warning (bool): If True, the message is treated as a warning and will
                        be logged even if the verbose mode is off, provided
                        the quiet mode is not enabled.
                        Defaults to False.

        Behavior:
        - Indents the message if there is a log indent level set and the log
        is not hanging.
        - Writes the message to the log file if it is set.
        - Writes the message to standard error output if verbose mode is
        enabled or if it is a warning
        and quiet mode is not enabled.
        - Flushes the log file and standard error output if the message does
        not end with a newline.
        - Sets the log hanging state based on whether the message ends with a
        newline.
        """
        should_indent = (self._logIndent > 0) and (not self._logHanging)
        if should_indent:
            if self._logFile:
                self._logFile.write(self._logIndent * "  ")
            if self._verbose or (warning and not self._quiet):
                sys.stderr.write(self._logIndent * "  ")
            self._logHanging = True

        if self._logFile:
            self._logFile.write(message)
        if self._verbose or (warning and not self._quiet):
            sys.stderr.write(message)

        if message[-1:] != "\n":
            if self._logFile:
                self._logFile.flush()
            if self._verbose or (warning and not self._quiet):
                sys.stderr.flush()
            self._logHanging = True
        else:
            self._logHanging = False

    def log(self, message=""):
        self._log(message, False)

    def logPush(self, message=None):
        if message:
            self.log(message)
        if self._logHanging:
            self.log("\n")
        self._logIndent += 1

    def logPop(self, message=None):
        if self._logHanging:
            self.log("\n")
        self._logIndent = max(0, self._logIndent - 1)
        if message:
            self.log(message)

    def warn(self, message=""):
        self._log(message, True)

    def warnPush(self, message=None):
        if message:
            self.warn(message)
        if self._logHanging:
            self.warn("\n")
        self._logIndent += 1

    def warnPop(self, message=None):
        if self._logHanging:
            self.warn("\n")
        self._logIndent = max(0, self._logIndent - 1)
        if message:
            self.warn(message)
